/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.Callback;

import java.sql.Timestamp;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientCallback implements Callback {

    private volatile TimestampMessage.TimestampResponse _response = null;
    private Exception _e = null;
    private CountDownLatch _latch = new CountDownLatch(1);

    ClientCallback() {
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

    public synchronized void error(Exception e) {
        _e = e;
        countDown();
    }

    public synchronized void complete(TimestampMessage.TimestampResponse response) {
        _response = response;
        countDown();
    }

    public boolean responseIsInvalid() {
        switch (_response.getTimestampRequestType()) {
            case GET_NEXT_TIMESTAMP:
                return _response.getGetNextTimestampResponse().getTimestamp() < 0;
            case GET_CURRENT_TIMESTAMP:
                return _response.getGetNextTimestampResponse().getTimestamp() < 0;
            case BUMP_TIMESTAMP:
            default:
                return false;
        }
    }

    public String toString() {
        return _response.toString();
    }

    public TimestampMessage.TimestampResponse getResponse() {
        return _response;
    }
}
