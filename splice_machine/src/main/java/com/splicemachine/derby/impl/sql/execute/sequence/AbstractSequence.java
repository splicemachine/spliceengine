package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;

public abstract class AbstractSequence implements Sequence, Externalizable {
	protected final AtomicLong remaining = new AtomicLong(0l);
    protected final AtomicLong currPosition = new AtomicLong(0l);
    protected long blockAllocationSize;
    protected long incrementSteps;
    protected final Lock updateLock = new ReentrantLock();
	protected long startingValue;

    public AbstractSequence() {

    }

    public AbstractSequence(long blockAllocationSize, long incrementSteps, long startingValue) {
    	if (incrementSteps > blockAllocationSize)
    		blockAllocationSize = incrementSteps;
        this.blockAllocationSize = blockAllocationSize;
        this.incrementSteps = incrementSteps;
        this.startingValue = startingValue;
    }

    public long getNext() throws StandardException {
        if(remaining.getAndDecrement()<=0)
            allocateBlock();
        return currPosition.getAndAdd(incrementSteps);
    }
    
    protected abstract long getCurrentValue() throws IOException;

    protected abstract boolean atomicIncrement(long nextValue) throws IOException;
    
    public abstract void close() throws IOException;

    private void allocateBlock() throws StandardException {
        boolean success = false;
        while(!success){
            updateLock.lock();
            try{
                if(remaining.getAndDecrement()>0)
                	return;
                currPosition.set(getCurrentValue());
                success = atomicIncrement(currPosition.get()+(incrementSteps>blockAllocationSize?incrementSteps:blockAllocationSize));
                if(success) {
                    remaining.set(blockAllocationSize/incrementSteps - 1);
                }
            } 
            catch (IOException e) {
                throw Exceptions.parseException(e);
            } 
            finally{
                updateLock.unlock();
            }
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(blockAllocationSize);
        out.writeLong(incrementSteps);
        out.writeLong(startingValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        blockAllocationSize = in.readLong();
        incrementSteps = in.readLong();
        startingValue = in.readLong();
    }
}
