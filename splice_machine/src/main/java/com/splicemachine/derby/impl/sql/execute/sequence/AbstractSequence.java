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

package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractSequence implements Sequence, Externalizable{
    protected final AtomicLong remaining=new AtomicLong(0l);
    protected final AtomicLong currPosition=new AtomicLong(0l);
    protected long blockAllocationSize;
    protected long incrementSteps;
    protected final Lock updateLock=new ReentrantLock();
    protected long startingValue;

    public AbstractSequence(){

    }

    public AbstractSequence(long blockAllocationSize,long incrementSteps,long startingValue){
        if(incrementSteps>blockAllocationSize)
            blockAllocationSize=incrementSteps;
        this.blockAllocationSize=blockAllocationSize;
        this.incrementSteps=incrementSteps;
        this.startingValue=startingValue;
    }

    public long getNext() throws StandardException{
        if(remaining.getAndDecrement()<=0)
            allocateBlock(false);
        return currPosition.getAndAdd(incrementSteps);
    }

    public long peekAtCurrentValue() throws StandardException {
        if(remaining.get()<= 0)
            allocateBlock(true);
        return currPosition.get();
    }

    protected abstract long getCurrentValue() throws IOException;

    protected abstract boolean atomicIncrement(long nextValue) throws IOException;

    public abstract void close() throws IOException;

    private void allocateBlock(boolean peek) throws StandardException{
        boolean success=false;
        while(!success){
            updateLock.lock();
            try{
                if(remaining.getAndDecrement()>0)
                    return;
                currPosition.set(getCurrentValue());
                success=atomicIncrement(currPosition.get()+(incrementSteps>blockAllocationSize?incrementSteps:blockAllocationSize));
                if(success){
                    long v = blockAllocationSize/incrementSteps;
                    remaining.set(peek?v:v-1);
                }
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }finally{
                updateLock.unlock();
            }
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeLong(blockAllocationSize);
        out.writeLong(incrementSteps);
        out.writeLong(startingValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        blockAllocationSize=in.readLong();
        incrementSteps=in.readLong();
        startingValue=in.readLong();
    }
}
