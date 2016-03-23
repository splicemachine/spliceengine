package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.api.PipelineMeter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class CountingPipelineMeter implements PipelineMeter{
    private final AtomicLong rejectedCount = new AtomicLong(0l);

    private final AtomicLong successCounter = new AtomicLong(0l);
    private final AtomicLong failedCounter = new AtomicLong(0l);
    private final long startupTimestamp = System.nanoTime();

    @Override
    public void mark(int numSuccess,int numFailed){
        successCounter.addAndGet(numSuccess);
        failedCounter.addAndGet(numFailed);
    }

    @Override
    public void rejected(int numRows){
        rejectedCount.addAndGet(numRows);
    }

    @Override
    public double throughput(){
        return ((double)successCounter.get())/(System.nanoTime()-startupTimestamp);
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
        return rejectedCount.get();
    }
}
