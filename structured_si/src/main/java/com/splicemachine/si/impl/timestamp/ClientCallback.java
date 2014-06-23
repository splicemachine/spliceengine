package com.splicemachine.si.impl.timestamp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

public class ClientCallback implements Callback {
	
    private final short _callerId; 
	private volatile long _newTimestamp = -1l;
	private Exception _e = null;
    private CountDownLatch _latch = new CountDownLatch(1);
    		
    public ClientCallback(short callerId) {
    	_callerId = callerId;
    }
    
    public short getCallerId() {
    	return _callerId;
    }
    
    public Exception getException() {
       return _e;
    }

    protected void countDown() {
       _latch.countDown();
    }
    
    public boolean await(int timeoutMillis) throws InterruptedException {
        return _latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public void await() throws InterruptedException {
        _latch.await(SpliceConstants.timestampClientWaitTime, TimeUnit.MILLISECONDS);
    }

    public long getNewTimestamp() {
    	return _newTimestamp;
    }

    public synchronized void error(Exception e) {
        _e = e;
        countDown();
    }

    public synchronized void complete(long timestamp) {
        _newTimestamp = timestamp;
        countDown();
    }   

    public String toString() {
    	return "Callback (callerId = " + _callerId +
    		(_newTimestamp > -1 ? ", ts = " + _newTimestamp : ", ts blank") + ")";
    }
    
}    
