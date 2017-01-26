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

package com.splicemachine.pipeline.traffic;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.primitives.Bytes;

/**
 * @author Scott Fines
 *         Date: 1/15/16
 */
public class SynchronousWriteControl implements SpliceWriteControl{
    private volatile WriteStatus currStatus = new WriteStatus(0,0,0,0);

    private volatile int maxDependentWriteThreads;
    private volatile int maxIndependentWriteThreads;
    private volatile int maxDependentWriteCount;
    private volatile int maxIndependentWriteCount;

    public SynchronousWriteControl(int maxDependentWriteThreads,
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

    @Override
    public Status performDependentWrite(int writes){
        synchronized(this){
            //avoid the extra cost of a volatile read, since we're already synchronized
            WriteStatus ws = currStatus;
            if(ws.dependentWriteThreads>maxDependentWriteThreads ||
                    ws.dependentWriteCount>maxDependentWriteCount){
                return Status.REJECTED;
            }

            currStatus = WriteStatus.incrementDependentWriteStatus(currStatus,writes);
            return Status.DEPENDENT;
        }
    }

    @Override
    public boolean finishDependentWrite(int writes){
        synchronized(this){
            currStatus = WriteStatus.decrementDependentWriteStatus(currStatus,writes);
            return true;
        }
    }

    @Override
    public Status performIndependentWrite(int writes){
        synchronized(this){
            WriteStatus state = currStatus;
            if(state.independentWriteThreads>maxIndependentWriteThreads
                    ||state.independentWriteCount> maxIndependentWriteCount){
                return performDependentWrite(writes);
            }else{
                currStatus = WriteStatus.incrementIndependentWriteStatus(currStatus,writes);
                return Status.INDEPENDENT;
            }
        }
    }

    @Override
    public boolean finishIndependentWrite(int writes){
        synchronized(this){
            currStatus = WriteStatus.decrementIndependentWriteStatus(currStatus,writes);
            return true;
        }
    }

    @Override
    public WriteStatus getWriteStatus(){
        return currStatus;
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

    public static void main(String...args) throws Exception{
        byte[] b1 = new byte[]{0x00,0x01,0x02};
        byte[] b2 = new byte[]{0x00,0x20,0x02};
        System.out.println(ByteComparisons.comparator().compare(b1,b2));
    }
}
