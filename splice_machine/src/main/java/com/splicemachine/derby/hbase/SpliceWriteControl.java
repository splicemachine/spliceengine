package com.splicemachine.derby.hbase;
import java.util.concurrent.atomic.AtomicReference;

public class SpliceWriteControl {
	public enum Status {
		DEPENDENT, INDEPENDENT, REJECTED
	}
	private AtomicReference<WriteStatus> writeStatus = new AtomicReference<WriteStatus>(new WriteStatus(0,0,0,0));
    protected int maxDependentWriteThreads;
    protected int maxIndependentWriteThreads;    
    protected int maxDependentWriteCount;
    protected int maxIndependentWriteCount;    
    
    public SpliceWriteControl(int maxDependentWriteThreads, 
    		int maxIndependentWriteThreads,int maxDependentWriteCount, int maxIndependentWriteCount) {
    	assert (maxDependentWriteThreads>=0 &&
    			maxIndependentWriteThreads >= 0 &&
    			maxDependentWriteCount >= 0 &&
    					maxIndependentWriteCount >= 0);
    	this.maxIndependentWriteThreads  = maxIndependentWriteThreads;
    	this.maxDependentWriteThreads = maxDependentWriteThreads;
    	this.maxDependentWriteCount = maxDependentWriteCount;
    	this.maxIndependentWriteCount = maxIndependentWriteCount;
    }
    
    public Status performDependentWrite(int writes) {
    	while (true) {
    		WriteStatus state = writeStatus.get();
    		if (state.dependentWriteThreads > maxDependentWriteThreads || state.dependentWriteCount > maxDependentWriteCount)
    			return Status.REJECTED;
    		if (writeStatus.compareAndSet(state, WriteStatus.incrementDependentWriteStatus(state,writes)))
    			return Status.DEPENDENT;
    	}    		
    }

    public boolean finishDependentWrite(int writes) {
    	while (true) {
    		WriteStatus state = writeStatus.get();
    		if (writeStatus.compareAndSet(state, WriteStatus.decrementDependentWriteStatus(state,writes)))
    			return true;
    	}    		
    }
    
    public Status performIndependentWrite(int writes) {
    	while (true) {
    		WriteStatus state = writeStatus.get();
    		if (state.independentWriteThreads > maxIndependentWriteThreads || state.independentWriteCount > maxIndependentWriteCount)
    			return (performDependentWrite(writes)); // Attempt to steal
    		if (writeStatus.compareAndSet(state, WriteStatus.incrementIndependentWriteStatus(state,writes)))
    			return Status.INDEPENDENT;
    	}    
    }

    public boolean finishIndependentWrite(int writes) {
    	while (true) {
    		WriteStatus state = writeStatus.get();
    		if (writeStatus.compareAndSet(state, WriteStatus.decrementIndependentWriteStatus(state,writes)))
    			return true;
    	}    		
    }

	public AtomicReference<WriteStatus> getWriteStatus() {
		return writeStatus;
	}

	public void setWriteStatus(AtomicReference<WriteStatus> writeStatus) {
		this.writeStatus = writeStatus;
	}
	
}
