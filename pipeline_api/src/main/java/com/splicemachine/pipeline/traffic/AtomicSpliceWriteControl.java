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

package com.splicemachine.pipeline.traffic;

import java.util.concurrent.atomic.AtomicLong;

/**
 * WriteControl limits (or controls) the rate of writes per region server.  It restricts writes based on the number of writes that are currently "in flight"
 * and the number of writer threads that are currently in use.  WriteControl is essentially a multi-variable counting semaphore where the counting variables
 * are the number of current writes and the number of current writer threads.  The limiting variables (or buckets) or further subdivided into independent and 
 * dependent writes.  Independent writes being writes to a single table and dependent writes being writes that require multiple tables to written to such as
 * a base table and its indexes.  WriteControl does not actually perform writes.  It just controls whether or not the write is allowed to proceed.
 * It essentially gives out "permits" when the write request fits within the control limits and rejects write requests when they don't.
 */

public class AtomicSpliceWriteControl implements SpliceWriteControl{

    protected int maxDependentWriteThreads;
    protected int maxIndependentWriteThreads;
    protected int maxDependentWriteCount;
    protected int maxIndependentWriteCount;

    private final AtomicLong dependentWrites = new AtomicLong();
    private final AtomicLong independentWrites = new AtomicLong();

    public AtomicSpliceWriteControl(int maxDependentWriteThreads,
                                    int maxIndependentWriteThreads,int maxDependentWriteCount,int maxIndependentWriteCount) {
        assert (maxDependentWriteThreads >= 0 &&
                maxIndependentWriteThreads >= 0 &&
                maxDependentWriteCount >= 0 &&
                maxIndependentWriteCount >= 0);
        this.maxIndependentWriteThreads = maxIndependentWriteThreads;
        this.maxDependentWriteThreads = maxDependentWriteThreads;
        this.maxDependentWriteCount = maxDependentWriteCount;
        this.maxIndependentWriteCount = maxIndependentWriteCount;
    }

    static private int toThreads(long val) {
        return (int)(val >>> 32);
    }

    static private int toWrites(long val) {
        return (int)(val & 0xffffffffL);
    }

    static private long toValue(int threads, int writes) {
        return ((long)threads << 32) + writes;
    }

    @Override
    public Status performDependentWrite(int writes) {
        for (;;) {
            long val = dependentWrites.get();
            int threads = toThreads(val) + 1;
            int count = toWrites(val) + writes;
            if (threads > maxDependentWriteThreads || count > maxDependentWriteCount) {
                return Status.REJECTED;
            }
            if (dependentWrites.compareAndSet(val, toValue(threads, count))) {
                return Status.DEPENDENT;
            }
        }
    }

    @Override
    public boolean finishDependentWrite(int writes) {
        dependentWrites.addAndGet(-toValue(1, writes));
        return true;
    }

    @Override
    public Status performIndependentWrite(int writes) {
        for (;;) {
            long val = independentWrites.get();
            int threads = toThreads(val) + 1;
            int count = toWrites(val) + writes;
            if (threads > maxIndependentWriteThreads || count > maxIndependentWriteCount) {
                return performDependentWrite(writes);
            }
            if (independentWrites.compareAndSet(val, toValue(threads, count))) {
                return Status.INDEPENDENT;
            }
        }
    }

    @Override
    public boolean finishIndependentWrite(int writes) {
        independentWrites.addAndGet(-toValue(1, writes));
        return true;
    }

    @Override
    public WriteStatus getWriteStatus() {
        long depVal = dependentWrites.get();
        long indepVal = independentWrites.get();
        return new WriteStatus(toThreads(depVal), toWrites(depVal),
                toWrites(indepVal), toThreads(indepVal));
    }

    @Override
    public int maxDependendentWriteThreads(){
        return maxDependentWriteThreads;
    }

    @Override
    public int maxIndependentWriteThreads(){
        return maxIndependentWriteThreads;
    }

    @Override
    public int maxDependentWriteCount(){
        return maxDependentWriteCount;
    }

    @Override
    public int maxIndependentWriteCount(){
        return maxIndependentWriteCount;
    }

    @Override
    public void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads){
        this.maxIndependentWriteThreads = newMaxIndependentWriteThreads;
    }

    @Override
    public void setMaxDependentWriteThreads(int newMaxDependentWriteThreads){
        this.maxDependentWriteThreads = newMaxDependentWriteThreads;
    }

    @Override
    public void setMaxIndependentWriteCount(int newMaxIndependentWriteCount){
        this.maxIndependentWriteCount = newMaxIndependentWriteCount;
    }

    @Override
    public void setMaxDependentWriteCount(int newMaxDependentWriteCount){
        this.maxDependentWriteCount = newMaxDependentWriteCount;
    }

}
