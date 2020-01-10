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
 *
 */

package com.splicemachine.si.impl.server;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class SimpleCompactionContext implements CompactionContext {
    private static final Logger LOG = Logger.getLogger(SimpleCompactionContext.class);
    
    private AtomicLong readData = new AtomicLong();
    private AtomicLong recordResolutionCached = new AtomicLong();
    private AtomicLong readCommit = new AtomicLong();
    private AtomicLong recordResolutionScheduled = new AtomicLong();
    private AtomicLong rowRead = new AtomicLong();
    private AtomicLong recordTimeout = new AtomicLong();
    private AtomicLong recordUnresolvedTransaction = new AtomicLong();
    private AtomicLong recordResolutionRejected = new AtomicLong();
    private AtomicLong recordRPC = new AtomicLong();
    private AtomicLong timeBlocked = new AtomicLong();

    @Override
    public void readData() {
        readData.incrementAndGet();
    }

    @Override
    public void recordResolutionCached() {
        recordResolutionCached.incrementAndGet();
    }

    @Override
    public void readCommit() {
        readCommit.incrementAndGet();
    }

    @Override
    public void recordResolutionScheduled() {
        recordResolutionScheduled.incrementAndGet();
    }

    @Override
    public void rowRead() {
        rowRead.incrementAndGet();
    }

    @Override
    public void recordTimeout() {
        recordTimeout.incrementAndGet();
    }

    @Override
    public void recordUnresolvedTransaction() {
        recordUnresolvedTransaction.incrementAndGet();
    }

    @Override
    public void recordResolutionRejected() {
        recordResolutionRejected.incrementAndGet();
    }

    @Override
    public void recordRPC() {
        recordRPC.incrementAndGet();
    }

    @Override
    public void close() {
        LOG.info(toString());
    }

    @Override
    public void timeBlocked(long duration) {
        timeBlocked.addAndGet(duration);
    }

    @Override
    public String toString() {
        return "SimpleCompactionContext{" +
                "readData=" + readData +
                ", recordResolutionCached=" + recordResolutionCached +
                ", readCommit=" + readCommit +
                ", recordResolutionScheduled=" + recordResolutionScheduled +
                ", rowRead=" + rowRead +
                ", recordTimeout=" + recordTimeout +
                ", recordUnresolvedTransaction=" + recordUnresolvedTransaction +
                ", recordResolutionRejected=" + recordResolutionRejected +
                ", recordRPC=" + recordRPC +
                ", timeBlocked(ms)=" + timeBlocked +
                '}';
    }
}
