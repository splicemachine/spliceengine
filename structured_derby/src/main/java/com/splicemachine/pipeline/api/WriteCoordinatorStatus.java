package com.splicemachine.pipeline.api;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 8/14/13
 */
@MXBean
public interface WriteCoordinatorStatus {

    long getMaxBufferHeapSize();
    void setMaxBufferHeapSize(long newMaxHeapSize);

    int getMaxBufferEntries();
    void setMaxBufferEntries(int newMaxBufferEntries);

    int getOutstandingCallBuffers();

    int getMaximumRetries();

    void setMaximumRetries(int newMaxRetries);

    long getPauseTime();

    void setPauseTime(long newPauseTimeMs);

    int getMaxFlushesPerRegion();

    void setMaxFlushesPerRegion(int newMaxFlushesPerRegion);

    long getSynchronousFlushCount();
}
