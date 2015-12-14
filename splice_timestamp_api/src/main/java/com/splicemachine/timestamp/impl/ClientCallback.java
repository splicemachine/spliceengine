package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.Callback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    public boolean awaitUninterruptibly(long timeoutMillis){
        boolean interrupted =false;
        long timeRemaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while(timeRemaining>0){
            try{
                long t = System.nanoTime();
                if(_latch.await(timeRemaining,TimeUnit.NANOSECONDS)){
                    if(interrupted)
                        Thread.currentThread().interrupt(); //reset the interrupt flag
                    return true;
                }
                timeRemaining-=(System.nanoTime()-t);
            }catch(InterruptedException e){
                interrupted=true;
                Thread.interrupted(); //clear the interrupt status temporarily
            }
        }
        if(interrupted)
            Thread.currentThread().interrupt(); //reset the interrupt flag
        return false;
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
