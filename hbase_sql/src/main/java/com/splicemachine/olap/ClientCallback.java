package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientCallback implements Callback {

    private final short _callerId;
    private volatile long _newTimestamp = -1l;
    private Throwable _e = null;
    private CountDownLatch _latch = new CountDownLatch(1);

    public OlapResult getResult() {
        return result;
    }

    private OlapResult result;

    public ClientCallback(short callerId) {
    	_callerId = callerId;
    }
    
    public short getCallerId() {
    	return _callerId;
    }
    
    public Throwable getThrowable() {
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

    public synchronized void error(Throwable t) {
        _e = t;
        countDown();
    }

    public synchronized void complete(OlapResult result) {
        this.result = result;
        countDown();
    }   

    public String toString() {
    	return "Callback (callerId = " + _callerId +
    		(_newTimestamp > -1 ? ", ts = " + _newTimestamp : ", ts blank") + ")";
    }
    
}    
