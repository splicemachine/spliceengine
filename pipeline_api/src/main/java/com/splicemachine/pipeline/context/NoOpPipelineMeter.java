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
    @Override public double throughput(){ return 0; }
    @Override public double fifteenMThroughput(){ return 0; }
    @Override public double fiveMThroughput(){ return 0; }
    @Override public double oneMThroughput(){ return 0; }
}
