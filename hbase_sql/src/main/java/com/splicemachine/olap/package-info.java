/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * An "Olap Server" capable of taking requests.
 *
 * <h3>Compaction Failure states</h3>
 *
 * <h4>Synchronous Compactions</h4>
 * The initial design for compactions in spark made the individual HBase RegionServers contact the OlapServer using
 * synchronous communication between the Client and Server. That is, the flow went something like
 *
 * 1. client connects to server
 * 2. client issues compaction request
 * 3. server performs compaction directly on the network thread pool (using spark)
 * 4. server returns completion to client when finished, or throws error in the event of abort
 * 5. client waits for response
 *
 * This mechanism is useful in that it is very simple--we can outline it in just 5 steps. It also has few failure
 * conditions: If the server dies, the client immediately knows and can abort the compaction.
 *
 * However, it is problematic at scale:
 *
 * 1. The client has a timeout wait period, if the compaction takes too long, the client itself will simply disconnect,
 * resulting in compaction aborts just because the compaction took too long.
 * 2. All compactions are synchronous on the network thread. This has several problems:
 *  1. There can only be as many compactions running as there are threads in the network thread pool. This forces us to either
 *     A) allow an unlimited thread pool (and thus risk running out of threads during periods of very high load), or
 *     B) force any compactions to come in above the thread pool size to wait their turn.
 *   In case A) we have a danger of running out of native threads on the OlapServer itself. This type of failure is dangerous
 *   because it doesn't necessarily kill the OlapServer--we would end up with a limping Server which is unable to serve
 *   any requests and therefore no compactions can complete.
 *
 *   In case B), we can only have as many compactions running at one time as there are network threads available. This limit
 *   is disassociated from the resource limits imposed by YARN on spark.
 *
 *   In both cases, time spent waiting for network resources (either to create a new thread, or to wait for an old compaction to complete)
 *   extends the amount of time taken by a single compaction to complete. This increases the likelihood of both network
 *   disconnect effects (like SocketTimeouts due to inactivity) and client-driven timeouts aborting compactions unnecessarily.
 * 4. If the client dies, the server continues to perform a compaction which is no longer necessary, because it has
 * no means of detecting client failures.
 *
 * <h4>Asynchronous Compactions</h4>
 * Because of the problems noted in synchronous compactions, it is preferable to perform asynchronous compactions.
 * However, from the standpoint of calls to compact() within Hbase, a compaction must still appear synchronous. Therefore,
 * the SyncOlapClient must still behave synchronously, while the client-server interaction is asynchronous.
 *
 * In this model, we do the following:
 *
 * 1. submit compaction request to server
 * 2. Server pushes compaction request to background execution queue (i.e. asynchronous execution)
 * 3. Server responds as soon as request is pushed with a unique id of the compaction, as well as a <em>tick time</em>.
 * This tick time tells the client how long to wait before checking back in on the status of the compaction.
 * 4. The client goes to sleep for the tick time, then sends out a <em>status</em> request to the server with the
 * unique id of the compaction.
 * 5. Upon receiving a status request, the server checks on the status of the uniquely determined compaction and
 * returns the status. If the compaction completed successfully, it returns an object holding the new compacted HFile names etc.
 * If the compaction failed, the object holds an object containing information about the error itself (i.e. the throwable).
 * 6. The client reacts accordingly.
 *
 * This is obviously more complicated, in that it has a <em>SUBMIT</em> and a <em>STATUS</em> network request pattern,
 * but it eliminates 3 major problems with synchronous execution:
 *
 * 1. No matter how long a compaction takes, the client is able to wait without worrying about socket timeouts, reconnects,
 * or anything like that
 * 2. spark resources are bounded by YARN resource allocation by design (and not subject to accidental misconfigurations
 * which may violate that).
 * 3. network resources can remain bounded without limiting throughput
 * 4. server death can be detected by either a connection exception during a STATUS or SUBMIT call.
 * 5. In the event of a SocketTimeout exception or other connection problem, STATUS calls can be retried without fear.
 * SUBMIT calls can also be made idempotent with a careful choice of unique identifier (i.e. a hash code of the file names
 * to be compacted would be one option).
 * 6. The server can use failure detector and heartbeat algorithms to detect client death and cancel appropriately.
 *
 * <h4>Dealing with Server death and failover</h4>
 * Asynchronous communication unfortunately adds complexity in handling failure conditions. There are two particular
 * instances where a failure of the OlapServer can be problematic: outright failure and failover to a backup.
 *
 * The client will detect an outright failure (i.e. no OlapServer is running anymore, or a network partition makes the
 * server unreachable by the client) immediately upon the next network request, because it will get either a Connection
 * or socket-related TCP error. When this happens, the client should retry a few times to ensure that it is not a temporary
 * failure and the server may suddenly become available. If it is not available after the retry window, then the client
 * should abort the compaction. This retry pattern does require that the SUBMIT call be idempotent, so that
 * accidentally attempting to submit it twice will have no effect.
 *
 * In the event of a failover, the OlapServer will become unavailable for a period of time, and then become available
 * again, but in in-memory state (i.e. the status of any running compaction job) will be lost (-sf- hopefully
 * Spark will automatically cancel running jobs which suddenly lose their coordinator, but we'll want to make sure of it).
 * Therefore, in the case of a STATUS call, the client will suddenly go from a RUNNING state to a NOT_SUBMITTED state.
 * This can be used in one of two ways: the compaction can be resubmitted, or the compaction can be aborted as failed.
 *
 * <h4>Client timeout and cancellation</h4>
 * It is possible in some cases for HBase to want to cancel the compaction: either because it was aborted
 * for some other reason, or because the server itself is shutting down. To facilitate that, we want the client
 * to send a <em>CANCEL</em> request containing the unique identifier. In that case, any running compaction will
 * be directly cancelled.
 *
 * This has an added usefulness in that we can retain behavior and force clients to timeout compactions if we so chose
 * (i.e. we could allow clients to not wait for forever).
 *
 * <h4>Detecting Client Failure</h4>
 * It is imperative that we detect client failure. The STATUS call that the client makes can be used as a heartbeat,
 * and there are several failure detection algorithms for heartbeats: either a strict timeout, or a phi detector:
 *
 * 1. a Strict timeout says if more than X heartbeats have gone missing, then the client is dead
 * 2. the phi detector uses the Phi detection algorithm(http://www.jaist.ac.jp/~defago/files/pdf/IS_RR_2004_010.pdf) to
 * determine whether the client is really dead.
 *
 * The phi detector is more reliable, but strict timeout may be simpler and easier to implement (at least initially).
 *
 * In either case, once a client is deemed to be dead, its related compaction must be cancelled. This death
 * check should be performed periodically by the compaction execution thread itself (i.e. stick its nose up in the air
 * periodically to detect whether or not it should continue working).
 *
 *
 * @author Scott Fines
 * Date: 3/31/16
 */
package com.splicemachine.olap;
