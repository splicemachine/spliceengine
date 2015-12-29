package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.api.PipelineMeter;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class YammerPipelineMeter implements PipelineMeter{

    @Override
    public void mark(int numSuccess,int numFailed){
        //TODO -sf- implement
    }

    @Override
    public double throughput(){
        return 0;
    }

    @Override
    public double fifteenMThroughput(){
        return 0;
    }

    @Override
    public double fiveMThroughput(){
        return 0;
    }

    @Override
    public double oneMThroughput(){
        return 0;
    }

    @Override
    public long rejectedCount(){
        return 0;
    }
}
