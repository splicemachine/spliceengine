package com.splicemachine.pipeline.traffic;

/**
 * @author Scott Fines
 *         Date: 1/15/16
 */
public interface SpliceWriteControl{
    enum Status {
        DEPENDENT, INDEPENDENT,REJECTED
    }

    Status performDependentWrite(int writes);

    boolean finishDependentWrite(int writes);

    Status performIndependentWrite(int writes);

    boolean finishIndependentWrite(int writes);

    WriteStatus getWriteStatus();

    int maxDependendentWriteThreads();

    int maxIndependentWriteThreads();

    int maxDependentWriteCount();

    int maxIndependentWriteCount();

    void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads);

    void setMaxDependentWriteThreads(int newMaxDependentWriteThreads);

    void setMaxIndependentWriteCount(int newMaxIndependentWriteCount);

    void setMaxDependentWriteCount(int newMaxDependentWriteCount);
}
