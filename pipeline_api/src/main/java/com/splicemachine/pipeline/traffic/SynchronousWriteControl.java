/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
