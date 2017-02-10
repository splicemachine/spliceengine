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

package com.splicemachine.concurrent.traffic;

import java.util.concurrent.TimeUnit;

/**
 * A mechanism for controlling or managing congestion of some resource (usually
 * bandwidth).
 *
 * In general, when one wishes to perform an action, one would attempt to
 * "acquire" {@code N} permits within a specific timeframe.
 * If {@code N} permits are available, then the action is considered
 * <em>conforming</em>, and is allowed to proceed. Otherwise, the action
 * is considered <em>non-conforming</em>. It is possible for fewer than {@code N}
 * permits to be available, in which case the action is considered <em>partially-conforming</em>.
 *
 * There are several strategies for dealing with non-conforming and partially-conforming actions:
 *
 * 1. Reject all non- and partially-conforming actions. That is, the action is not allowed to proceed
 * unless it is full conforming (This is generally what is referred to as "Traffic Policing")
 * 2. Delay all non- and partially-conforming actions until they are fully conforming (which is usually
 * referred to as "Traffic Shaping").
 * 3. Reduce priority of non- and partially-conforming actions (Also sometimes referred to as "Traffic Policing").
 *
 * Alternatively, some implementations may proactively perform any of the above actions on some conforming
 * actions, in order to allow better overall performance. For example, an implementation may probabilistically
 * reject some actions even though they are conforming; doing this may allow more actions to conform overall,
 * thus improving throughput.
 *
 * Implementations are not <em>required</em> to be thread-safe, although it is hard to imagine a scenario
 * in which a non-thread-safe CongestionControl might be useful. Nonetheless, as it is hard to predict
 * what uses people may have for the class, thread-safety requirements are delegated to the individual
 * implementations.
 *
 * @author Scott Fines
 *         Date: 11/12/14
 */
public interface TrafficController {

    /**
     * Get statistics about the traffic flow through this controller, if statistics
     * collection is enabled.
     *
     * Some implementations will choose not to collect statistics, will collect only
     * partial statistics, or may have statistics optionally disabled. In these cases,
     * the returns TrafficStats should return "zeros" for all values (or some other equivalent
     * value)
     *
     * @return Stats about the traffic flow throw this controller.
     */
    TrafficStats stats();

    /**
     * Acquire {@code numPermits} permits, waiting as long as necessary for that
     * many permits to become available.
     *
     * @param numPermits the number of permits to acquire
     * @throws java.lang.InterruptedException if the thread is interrupted
     * while acquiring permits
     */
    void acquire(int numPermits) throws InterruptedException;


    /**
     * Try to acquire {@code numPermits} permits immediately. If {@code numPermits} cannot
     * be immediately acquired, no permits are acquired and this method returns {@code false}.
     *
     * This method does <em>not</em> force the thread to wait for any time--either the permits
     * are immediately available, or this method will return false.
     *
     * @param numPermits the number of permits to acquire
     * @return true if {@code numPermits} permits are immediately available, false if the number of
     * permits cannot be acquired.
     */
    boolean tryAcquire(int numPermits);

    /**
     * Try to acquire {@code numPermits} permits, waiting up to {@code timeout} units of time
     * for permits to become available.
     *
     * If {@code timeout} time units expires and this call is still unable to acquire {@code numPermits}
     * permits, then this method returns {@code false}, and no tokens are acquired.
     *
     * It is up to the implementation to force a thread to wait when it is known that it will return false--
     * that is, an implementation may choose to return early if it determines that it is not possible
     * to acquire {@code numPermit} permits within the time allotted. For example, if one requests 10 permits,
     * but the implementation knows that it will only be able to generate 5 permits in the time allotted, it
     * may optionally return {@code false } immediately rather than forcing the thread to wait on an impossible
     * condition.
     *
     * @param numPermits the number of permits to acquire
     * @param timeout the amount of time to wait for {@code numPermits} to become available
     * @param timeUnit the unit of time to wait
     * @return {@code true} if {@code numPermits} were available within the timeout specified, {@code false} otherwise.
     */
    boolean tryAcquire(int numPermits, long timeout, TimeUnit timeUnit) throws InterruptedException;

    /**
     * acquire as many permits as possible within the time allotted.
     *
     * Upon entry, if {@code maxPermits} permits are available, then this method
     * will return immediately. Otherwise, the thread will wait up to the timeout,
     * and then return as many permits as possible.
     *
     * If it is not possible to acquire at least {@code minPermits} within the time
     * allotted, then no permits will be acquired. It is the option of the implementation
     * whether or not to force the thread to wait the full timeout in those cases (most
     * implementations probably won't).
     *
     * @param minPermits the minimum number of permits to acquire
     * @param maxPermits the maximum number of permits to acquire
     * @param timeout the maximum time to wait before returning
     * @param timeUnit the unit for the time
     * @return the number of permits acquired within the specified timeout. If the number
     * which would have been acquired is less that {@code minPermits}, then no permits are acquired
     * and this returns 0.
     */
    int acquire(int minPermits, int maxPermits, long timeout, TimeUnit timeUnit) throws InterruptedException;

    int tryAcquire(int minPermits, int maxPermits);

    /**
     * @return the number of permits currently available for requesting. This is a snapshot
     * in time, and should not be used for anything other than monitoring, as it will not reliably
     * tell you whether or not you can acquire any permits.
     */
    int availablePermits();

    /**
     * @return the maximum number of permits that can be stored (e.g. the max throughput of the system)
     */
    int maxPermits();

    /**
     * Optional method: Set a new max number of permits to use.
     *
     * Implementations are not required to make this happen immediately: for example, if
     * more permits than this are already set, then implementations may wait until those permits
     * are exhausted before setting the new maximum.
     *
     * @param newMaxPermits the new number of max permits
     * @throws java.lang.IllegalArgumentException if the new max permits is < 1
     */
    void setMaxPermits(int newMaxPermits);
}
