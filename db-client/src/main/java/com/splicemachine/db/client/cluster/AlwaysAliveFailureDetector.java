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
 * @author Scott Fines
 *         Date: 11/17/16
 */
class AlwaysAliveFailureDetector implements FailureDetector{
    private volatile boolean killed = false;

    static final FailureDetectorFactory FACTORY =AlwaysAliveFailureDetector::new;

    @Override public void success(){ killed = false; }

    @Override
    public void failed(){
        //no-op
    }

    @Override
    public double failureProbability(){
        return killed? 1: 0;
    }

    @Override
    public boolean isAlive(){
        return !killed;
    }

    @Override
    public void kill(){
        killed = true;
    }
}

