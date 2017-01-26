/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.services.cache;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.DaemonService;
import com.splicemachine.db.iapi.services.daemon.Serviceable;

/**
 * A background cleaner that {@code ConcurrentCache} can use to clean {@code
 * Cacheable}s asynchronously in a background instead of synchronously in the
 * user threads. It is normally used by the replacement algorithm in order to
 * make dirty {@code Cacheable}s clean and evictable in the future. When the
 * background cleaner is asked to clean an item, it puts the item in a queue
 * and requests to be serviced by a <code>DaemonService</code> running in a
 * separate thread.
 */
final class BackgroundCleaner implements Serviceable {

    /** The service thread which performs the clean operations. */
    private final DaemonService daemonService;

    /** Subscription number for this <code>Serviceable</code>. */
    private final int clientNumber;

    /**
     * Flag which tells whether the cleaner has a still unprocessed job
     * scheduled with the daemon service. If this flag is <code>true</code>,
     * calls to <code>serviceNow()</code> won't result in the cleaner being
     * serviced.
     */
    private final AtomicBoolean scheduled = new AtomicBoolean();

    /** A queue of cache entries that need to be cleaned. */
    private final ArrayBlockingQueue<CacheEntry> queue;

    /**
     * Flag which tells whether the cleaner should try to shrink the cache
     * the next time it wakes up.
     */
    private volatile boolean shrink;

    /** The cache manager owning this cleaner. */
    private final ConcurrentCache cacheManager;

    /**
     * Create a background cleaner instance and subscribe it to a daemon
     * service.
     *
     * @param cache the cache manager that owns the cleaner
     * @param daemon the daemon service which perfoms the work
     * @param queueSize the maximum number of entries to keep in the queue
     * (must be greater than 0)
     */
    BackgroundCleaner(
            ConcurrentCache cache, DaemonService daemon, int queueSize) {
        queue = new ArrayBlockingQueue<CacheEntry>(queueSize);
        daemonService = daemon;
        cacheManager = cache;
        // subscribe with the onDemandOnly flag
        clientNumber = daemon.subscribe(this, true);
    }

    /**
     * Try to schedule a clean operation in the background cleaner.
     *
     * @param entry the entry that needs to be cleaned
     * @return <code>true</code> if the entry has been scheduled for clean,
     * <code>false</code> if the background cleaner can't clean the entry (its
     * queue is full)
     */
    boolean scheduleClean(CacheEntry entry) {
        final boolean queued = queue.offer(entry);
        if (queued) {
            requestService();
        }
        return queued;
    }

    /**
     * Request that the cleaner tries to shrink the cache the next time it
     * wakes up.
     */
    void scheduleShrink() {
        shrink = true;
        requestService();
    }

    /**
     * Notify the daemon service that the cleaner needs to be serviced.
     */
    private void requestService() {
        // Calling serviceNow() doesn't have any effect if we have already
        // called it and the request hasn't been processed yet. Therefore, we
        // only call serviceNow() if we can atomically change scheduled from
        // false to true. If the cleaner is waiting for service (schedule is
        // true), we don't need to call serviceNow() since the cleaner will
        // re-request service when it finishes its current operation and
        // detects that there is more work in the queue.
        if (scheduled.compareAndSet(false, true)) {
            daemonService.serviceNow(clientNumber);
        }
    }

    /**
     * Stop subscribing to the daemon service.
     */
    void unsubscribe() {
        daemonService.unsubscribe(clientNumber);
    }

    /**
     * Clean the first entry in the queue. If there is more work, re-request
     * service from the daemon service.
     *
     * @param context ignored
     * @return status for the performed work (normally
     * <code>Serviceable.DONE</code>)
     * @throws StandardException if <code>Cacheable.clean()</code> fails
     */
    public int performWork(ContextManager context) throws StandardException {
        // allow others to schedule more work
        scheduled.set(false);

        // First, try to shrink the cache if requested.
        if (shrink) {
            shrink = false;
            cacheManager.getReplacementPolicy().doShrink();
        }

        // See if there are objects waiting to be cleaned.
        CacheEntry e = queue.poll();
        if (e != null) {
            try {
                cacheManager.cleanEntry(e);
            } finally {
                if (!queue.isEmpty() || shrink) {
                    // We have more work in the queue. Request service again.
                    requestService();
                }
            }
        }
        return Serviceable.DONE;
    }

    /**
     * Indicate that we want to be serviced ASAP.
     * @return <code>true</code>
     */
    public boolean serviceASAP() {
        return true;
    }

    /**
     * Indicate that we don't want the work to happen immediately in the
     * user thread.
     * @return <code>false</code>
     */
    public boolean serviceImmediately() {
        // This method isn't actually used by BasicDaemon, but we still need to
        // implement it in order to satisfy the Serviceable interface.
        return false;
    }
}
