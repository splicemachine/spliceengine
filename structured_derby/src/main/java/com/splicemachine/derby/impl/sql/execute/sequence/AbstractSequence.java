package com.splicemachine.derby.impl.sql.execute.sequence;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.derby.iapi.error.StandardException;

import com.splicemachine.pipeline.exception.Exceptions;

public abstract class AbstractSequence implements Sequence {
	protected final AtomicLong remaining = new AtomicLong(0l);
    protected final AtomicLong currPosition = new AtomicLong(0l);
    protected final long blockAllocationSize;
    protected final long incrementSteps;
    protected final Lock updateLock = new ReentrantLock();
	protected long startingValue;

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
    
}
