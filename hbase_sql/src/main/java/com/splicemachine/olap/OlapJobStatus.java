/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.olap;

import akka.remote.FailureDetector;
import akka.remote.FailureDetector$;
import akka.remote.PhiAccrualFailureDetector;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class OlapJobStatus implements OlapStatus{
    private final FailureDetector failureDetector;
    private final long tickTime;

    private volatile AtomicReference<OlapStatus.State> currentState = new AtomicReference<>(State.NOT_SUBMITTED);
    private volatile OlapResult results;

    public OlapJobStatus(long tickTime,int numTicks){
        //TODO -sf- remove the constants
        FiniteDuration maxHeartbeatInterval = FiniteDuration.apply(numTicks*tickTime,TimeUnit.MILLISECONDS);
        FiniteDuration stdDev = FiniteDuration.apply(2*tickTime,TimeUnit.MILLISECONDS);
        FiniteDuration firstTick = FiniteDuration.apply(tickTime,TimeUnit.MILLISECONDS);

        failureDetector = new PhiAccrualFailureDetector(0.1d,128,stdDev,maxHeartbeatInterval,firstTick,
                FailureDetector$.MODULE$.defaultClock());
        this.tickTime = tickTime;
    }

    public State checkState(){
        failureDetector.heartbeat();
        return currentState.get();
    }

    public OlapResult getResult(){
        State curState = currentState();
        switch(curState){
            case NOT_SUBMITTED:
            case SUBMITTED:
                return new SubmittedResult(tickTime);
            case RUNNING:
                return new ProgressResult();
            default:
                return results;
        }
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
        results = new CancelledResult();
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
                    assert results == result: "Programmer error: marked complete twice with two different results";
                    return;
            }
            shouldContinue = !currentState.compareAndSet(currState,State.COMPLETE);
        }while(shouldContinue);
        results = result;
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
                results=new FailedOlapResult(new TimeoutException("Client timed out response, assuming it died"));
                currentState.compareAndSet(curState,State.FAILED); //all other states don't have to be marked failed
                curState=State.FAILED;
            }
        }
        return curState;
    }


}
