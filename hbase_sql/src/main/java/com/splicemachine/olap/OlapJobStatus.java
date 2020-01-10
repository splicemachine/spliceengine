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

package com.splicemachine.olap;

import akka.remote.FailureDetector;
import akka.remote.FailureDetector$;
import akka.remote.PhiAccrualFailureDetector;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.log4j.Logger;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class OlapJobStatus implements OlapStatus{
    private static final Logger LOG = Logger.getLogger(OlapJobStatus.class);

    private final FailureDetector failureDetector;
    private final long tickTime;

    private volatile AtomicReference<OlapStatus.State> currentState = new AtomicReference<>(State.NOT_SUBMITTED);
    private ArrayBlockingQueue<OlapResult> results;
    private volatile OlapResult cachedResult;

    public OlapJobStatus(long tickTime,int numTicks){
        //TODO -sf- remove the constants
        FiniteDuration maxHeartbeatInterval = FiniteDuration.apply(numTicks*tickTime,TimeUnit.MILLISECONDS);
        FiniteDuration stdDev = FiniteDuration.apply(2*tickTime,TimeUnit.MILLISECONDS);
        FiniteDuration firstTick = FiniteDuration.apply(tickTime,TimeUnit.MILLISECONDS);

        failureDetector = new PhiAccrualFailureDetector(10,128,stdDev,maxHeartbeatInterval,firstTick,
                FailureDetector$.MODULE$.defaultClock());
        this.tickTime = tickTime;
        this.results = new ArrayBlockingQueue<>(1);
    }

    public State checkState(){
        failureDetector.heartbeat();
        return currentState.get();
    }

    public OlapResult getResult() {
        State curState = currentState();
        switch(curState){
            case NOT_SUBMITTED:
            case SUBMITTED:
                return new SubmittedResult(tickTime);
            case RUNNING:
                return new ProgressResult();
            default:
                return cacheResult();
        }
    }

    private OlapResult cacheResult() {
        assert currentState.get() == State.COMPLETE;
        if (cachedResult != null) {
            return cachedResult;
        }
        cachedResult = results.poll();
        assert cachedResult != null;
        return cachedResult;
    }

    public void cancel(){
        boolean shouldContinue;
        do{
            State currState = currentState.get();
            switch(currState){
                case NOT_SUBMITTED:
                case CANCELED:
                case FAILED:
                case COMPLETE:
                    return;
            }
            shouldContinue = !currentState.compareAndSet(currState,State.CANCELED);
        }while(shouldContinue);
        results.offer(new CancelledResult());
    }

    public boolean isAvailable(){
        return failureDetector.isAvailable();
    }

    public boolean markSubmitted(){
        boolean shouldContinue;
        do{
            State currState = currentState.get();
            switch(currState){
                case SUBMITTED:
                case RUNNING:
                case CANCELED:
                case FAILED:
                case COMPLETE:
                    return false;
            }
            shouldContinue = !currentState.compareAndSet(currState,State.SUBMITTED);
        }while(shouldContinue);
        return true;
    }

    public void markCompleted(OlapResult result){
        boolean shouldContinue;
        do{
            State currState = currentState.get();
            switch(currState){
                case NOT_SUBMITTED:
                case CANCELED:
                case FAILED:
                    return;
                case COMPLETE: //we've marked it complete twice
                    assert results.peek() == result: "Programmer error: marked complete twice with two different results";
                    return;
            }
            shouldContinue = !currentState.compareAndSet(currState,State.COMPLETE);
        }while(shouldContinue);
        results.offer(result);
    }

    public boolean markRunning(){
        boolean shouldContinue;
        do{
            State currState = currentState();
            switch(currState){
                case RUNNING: //we are already running, don't bother doing it again
                case CANCELED: //we were cancelled, so don't run
                case FAILED: //somehow, we are already marked as having failed--probably client timeout
                case COMPLETE:
                    return false;
            }
            shouldContinue = !currentState.compareAndSet(currState,State.RUNNING);
        }while(shouldContinue);
        return true;
    }

    @Override
    public boolean isRunning(){
        /*
         * Returns true if the job is still considered to be running (i.e. the client is still checking in regularly)
         */
        return currentState()==State.RUNNING;
    }

    @Override
    public boolean wait(long time, TimeUnit unit) throws InterruptedException {
        OlapResult result = results.poll(time, unit);
        if (result != null) {
            cachedResult = result;
        }
        return cachedResult != null;
    }

    /*package-private methods*/
    /* ****************************************************************************************************************/
    State currentState(){
        /*
         * Get the current state of the job. If the job has timed out because the waiting client has died (or
         * is presumed to be dead), then this will return FAILED.
         */
        return checkFailed();
    }

    /*private helper methods*/
    /* ****************************************************************************************************************/

    private State checkFailed(){
        /*
         * Checks whether the current state is failed or not. Used by internal compaction checking to determine
         * if the client has timed out or not.
         */
        State curState = currentState.get();
        if(curState==State.RUNNING){
            /*
             * This task is still running, so we need to check to see if the responsible client has died
             * or not.
             */
            if(!failureDetector.isAvailable()){
                results.offer(new FailedOlapResult(new TimeoutException("Client timed out response, assuming it died")));
                currentState.compareAndSet(curState,State.FAILED); //all other states don't have to be marked failed
                curState=State.FAILED;
            }
        }
        return curState;
    }

    @Override
    public String toString() {
        return "OlapJobStatus{" +
                "currentState=" + currentState +
                ", failureDetector.phi =" + ((PhiAccrualFailureDetector)failureDetector).phi() +
                '}';
    }
}
