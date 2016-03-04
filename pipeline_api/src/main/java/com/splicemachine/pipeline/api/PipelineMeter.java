package com.splicemachine.pipeline.api;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public interface PipelineMeter{

    void mark(int numSuccess, int numFailed);

    void rejected(int numRows);

    double throughput();

    double fifteenMThroughput();

    double fiveMThroughput();

    double oneMThroughput();

    long rejectedCount();
}
