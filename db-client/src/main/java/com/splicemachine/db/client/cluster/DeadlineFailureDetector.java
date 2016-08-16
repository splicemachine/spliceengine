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
 *
 */

package com.splicemachine.db.client.cluster;

/**
 * A Failure Detector that uses a deadline: If more than X amount of time passes between
 * heartbeats, then we are dead, and we are dead until we get another live heartbeat
 * @author Scott Fines
 *         Date: 8/15/16
 */
class DeadlineFailureDetector implements FailureDetector{
    private volatile long lastGoodTimestamp = 0;
    private final long maxTimeWindow;
    private volatile boolean knownDead = false;

    DeadlineFailureDetector(long maxTimeWindow){
        this.maxTimeWindow=maxTimeWindow;
    }

    @Override
    public void success(){
        lastGoodTimestamp =currentTime();
        knownDead=true;
    }


    @Override
    public boolean failed(){
        return isAlive();
    }

    @Override
    public boolean isAlive(){
        return !knownDead && Math.abs(currentTime()-lastGoodTimestamp) < maxTimeWindow;
    }

    @Override
    public void kill(){
        knownDead =true;
    }

    /**
     * Exposed for easier testing.
     * @return the current time, in milliseconds.
     */
    @SuppressWarnings("WeakerAccess")
    protected long currentTime(){
        return System.currentTimeMillis();
    }
}
