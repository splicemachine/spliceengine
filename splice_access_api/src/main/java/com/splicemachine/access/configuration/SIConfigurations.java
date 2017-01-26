/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.access.configuration;

import com.splicemachine.primitives.Bytes;

/**
 * Repository for holding configuration keys for SI.
 * <p/>
 * Each specific architecture configuration should provide
 * a default value for each of these keys.
 *
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class SIConfigurations implements ConfigurationDefault {
    public static final String CONGLOMERATE_TABLE_NAME = "SPLICE_CONGLOMERATE";
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = Bytes.toBytes(CONGLOMERATE_TABLE_NAME);

    public static final String completedTxnCacheSize="splice.txn.completedTxns.cacheSize";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE=1<<17; // want to hold lots of completed transactions

    public static final String completedTxnConcurrency="splice.txn.completedTxns.concurrency";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY=64;

    public static final String TRANSACTION_KEEP_ALIVE_INTERVAL="splice.txn.keepAliveIntervalMs";
    public static final long DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL=15000L;

    public static final String TRANSACTION_TIMEOUT="splice.txn.timeout";
    public static final long DEFAULT_TRANSACTION_TIMEOUT=10*DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL; // 2.5 Minutes

    public static final String TRANSACTION_KEEP_ALIVE_THREADS="splice.txn.keepAliveThreads";
    public static final int DEFAULT_KEEP_ALIVE_THREADS=4;

    public static final String READ_RESOLVER_THREADS = "splice.txn.readresolver.threads";
    private static final int DEFAULT_READ_RESOLVER_THREADS = 4;

    public static final String READ_RESOLVER_QUEUE_SIZE = "splice.txn.readresolver.queueSize";
    private static final int DEFAULT_READ_RESOLVER_QUEUE_SIZE=1<<16;

    /*
     * We use lock-striping to manage concurrent modifications/reads to the Transaction table. That is,
     * each Transaction is grouped into a bucket, and in order to read or modify that transaction, you must
     * first acquire the lock for that bucket.
     *
     * As a general rule, the more stripes you have, the more parallelism you can sustain. However, there are
     * two limiting factors to this. The first is memory--each stripe requires a separate set of objects which
     * occupy heap space. The second is threading performance.
     *
     * By experimentation, I've determined that (on the Oracle 6 JVM, at least) a ReadWriteLock occupies ~250 bytes,
     * so the total memory occupied is ~250*STRIPES, where STRIPES is the number of stripes that we have. Thus,
     * we have a table of memory usage as follows:
     *
     * 16       --  ~4K
     * 32       --  ~8K
     * 64       --  ~16K
     * 128      --  ~32K
     * 256      --  ~64K
     * 512      --  ~125K
     * 1024     --  ~250K
     * 4096     --  ~1M
     * 8192     --  ~2M
     * 16384    --  ~4M
     * 32768    --  ~8M
     *
     * This is the size for each transaction region, so there are actually 16 times that number of stripes (
     * and thus 16 times the memory usage).
     *
     * This inclines us to choose fewer stripes. However, we want to sustain a high degree of concurrency,
     * so we want to choose the correct number of stripes. Thankfully, we have a total limiter.
     *
     * All access to the transaction table occurs remotely (through the HBase client API), which means
     * that our maximum concurrency is actually the number of concurrent network actions that can be made
     * to a single server--in other words, the IPC threads. Any concurrency level which is higher than that
     * will be useless concurrency, as only a maximum of that many threads will be used. Thus, a reasonable
     * default is the number of ipc threads configured for this system.
     *
     * Note that the Stripe count is always a power of 2(if you set it to a non-power of 2, then the striper
     * will choose the smallest power of 2 greater than what you set), so we will always have a concurrency level
     * which is >= the number of ipc threads, which should allow plenty of concurrency for our applications.
     *
     * However, if we see bottlenecks due to this lock striping, then we may increase it manually, given the
     * tradeoffs that we discuss in this note.
     *
     */
    public static final String TRANSACTION_LOCK_STRIPES ="splice.txn.concurrencyLevel";

    /**
     * The number of milliseconds the timestamp client should wait for the response.
     * Defaults to 60000 (60 seconds)
     */
    public static final String TIMESTAMP_CLIENT_WAIT_TIME = "splice.timestamp_server.clientWaitTime";
    private static final int DEFAULT_TIMESTAMP_CLIENT_WAIT_TIME = 60000;

    /**
     * The Port to bind the Timestamp Server connection to
     * Defaults to 60012
     */
    public static final String TIMESTAMP_SERVER_BIND_PORT = "splice.timestamp_server.port";
    private static final int DEFAULT_TIMESTAMP_SERVER_BIND_PORT = 60012;


    /**
     * The number of milliseconds the OLAP client should wait for a result.
     * Defaults to Integer.MAX_VALUE (wait forever)
     */
    public static final String OLAP_CLIENT_WAIT_TIME = "splice.olap_server.clientWaitTime";
    private static final int DEFAULT_OLAP_CLIENT_WAIT_TIME = Integer.MAX_VALUE;

    /**
     * The number of milliseconds the OLAP client should wait for performing a status check.
     * Defaults to 1000 (1 s)
     */
    public static final String OLAP_CLIENT_TICK_TIME = "splice.olap_server.clientTickTime";
    private static final int DEFAULT_OLAP_CLIENT_TICK_TIME = 1000;

    /**
     * The Port to bind the OLAP Server connection to
     * Defaults to 60014
     */
    public static final String OLAP_SERVER_BIND_PORT = "splice.olap_server.port";
    private static final int DEFAULT_OLAP_SERVER_BIND_PORT = 60014;

    /**
     * Number of threads used by the Olap server, determines the maximum number of concurrent
     * Olap jobs
     *
     * Defaults to 16
     */
    public static final String OLAP_SERVER_THREADS = "splice.olap_server.threads";
    private static final int DEFAULT_OLAP_SERVER_THREADS = 16;

    public static final String OLAP_SERVER_TICK_LIMIT = "splice.olap_server.tickLimit";
    private static final int DEFAULT_OLAP_SERVER_TICK_LIMIT = 120;

    public static final String OLAP_CLIENT_RETRIES = "splice.olap_client.retries";
    private static final int DEFAULT_OLAP_CLIENT_RETRIES = 10;

    public static final String ACTIVE_TRANSACTION_CACHE_SIZE="splice.txn.activeCacheSize";
    private static final int DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE = 1<<12;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.completedTxnConcurrency  = configurationSource.getInt(completedTxnConcurrency, DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY);
        builder.completedTxnCacheSize  = configurationSource.getInt(completedTxnCacheSize, DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE);
        builder.transactionKeepAliveThreads  = configurationSource.getInt(TRANSACTION_KEEP_ALIVE_THREADS, DEFAULT_KEEP_ALIVE_THREADS);
        builder.readResolverThreads  = configurationSource.getInt(READ_RESOLVER_THREADS, DEFAULT_READ_RESOLVER_THREADS);
        builder.readResolverQueueSize  = configurationSource.getInt(READ_RESOLVER_QUEUE_SIZE, -1); //TODO -sf- reset to DEFAULT once ReadResolution works
//        builder.readResolverQueueSize  = configurationSource.getInt(READ_RESOLVER_QUEUE_SIZE, DEFAULT_READ_RESOLVER_QUEUE_SIZE);
        builder.timestampClientWaitTime  = configurationSource.getInt(TIMESTAMP_CLIENT_WAIT_TIME, DEFAULT_TIMESTAMP_CLIENT_WAIT_TIME);
        builder.timestampServerBindPort  = configurationSource.getInt(TIMESTAMP_SERVER_BIND_PORT, DEFAULT_TIMESTAMP_SERVER_BIND_PORT);
        builder.activeTransactionCacheSize  = configurationSource.getInt(ACTIVE_TRANSACTION_CACHE_SIZE, DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE);
        builder.olapServerBindPort  = configurationSource.getInt(OLAP_SERVER_BIND_PORT, DEFAULT_OLAP_SERVER_BIND_PORT);
        builder.olapClientWaitTime  = configurationSource.getInt(OLAP_CLIENT_WAIT_TIME, DEFAULT_OLAP_CLIENT_WAIT_TIME);
        builder.olapClientTickTime  = configurationSource.getInt(OLAP_CLIENT_TICK_TIME, DEFAULT_OLAP_CLIENT_TICK_TIME);
        builder.olapServerThreads = configurationSource.getInt(OLAP_SERVER_THREADS, DEFAULT_OLAP_SERVER_THREADS);
        builder.olapServerTickLimit = configurationSource.getInt(OLAP_SERVER_TICK_LIMIT,DEFAULT_OLAP_SERVER_TICK_LIMIT);
        builder.olapClientRetries = configurationSource.getInt(OLAP_CLIENT_RETRIES,DEFAULT_OLAP_CLIENT_RETRIES);

        builder.transactionTimeout = configurationSource.getLong(TRANSACTION_TIMEOUT, DEFAULT_TRANSACTION_TIMEOUT);
        builder.transactionKeepAliveInterval = configurationSource.getLong(TRANSACTION_KEEP_ALIVE_INTERVAL, DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL);

    }
}
