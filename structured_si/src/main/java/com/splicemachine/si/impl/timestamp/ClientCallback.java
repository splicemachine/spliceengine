package com.splicemachine.si.impl.timestamp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class ClientCallback implements Callback {
	
    private static final Logger LOG = Logger.getLogger(ClientCallback.class);

    private final int _callerId; 
	private volatile long _newTimestamp = -1l;
	private Exception _e = null;
    private CountDownLatch _latch = new CountDownLatch(1);
    		
    public ClientCallback(int callerId) {
    	_callerId = callerId;
    }
    
    public int getCallerId() {
    	return _callerId;
    }
    
    public Exception getException() {
       return _e;
    }

    protected void countDown() {
       _latch.countDown();
    }
    
    public void await() throws InterruptedException {
    	// TODO: decide on final timeout duration
        // _latch.await();
        _latch.await(30, TimeUnit.SECONDS);
    }

    public long getNewTimestamp() {
    	return _newTimestamp;
    }

    public synchronized void error(Exception e) {
        _e = e;
        countDown();
    }

    public synchronized void complete(long timestamp) {
    	TimestampUtil.doClientDebug(LOG, "complete: setting timestamp " + timestamp, this);
        _newTimestamp = timestamp;
        countDown();
    	TimestampUtil.doClientDebug(LOG, "complete: done with countdown", this);
    }   

    public String toString() {
    	return "Callback (callerId = " + _callerId + ", latch count = " +
            (_latch != null ? _latch.getCount() : "?") +
    		(_newTimestamp > -1 ? ", ts = " + _newTimestamp : ", ts blank") + ")";
    }
    
}    
