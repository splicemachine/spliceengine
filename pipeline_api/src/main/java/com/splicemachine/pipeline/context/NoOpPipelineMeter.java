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

package com.splicemachine.pipeline.context;

import com.splicemachine.pipeline.api.PipelineMeter;


/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class NoOpPipelineMeter implements PipelineMeter{
    public static final PipelineMeter INSTANCE = new NoOpPipelineMeter();

    private NoOpPipelineMeter(){}

    @Override public void mark(int numSuccess,int numFailed){ }

    @Override public void rejected(int numRows){ }

    @Override public double throughput(){ return 0; }
    @Override public double fifteenMThroughput(){ return 0; }
    @Override public double fiveMThroughput(){ return 0; }
    @Override public double oneMThroughput(){ return 0; }
    @Override public long rejectedCount(){ return 0; }
}
