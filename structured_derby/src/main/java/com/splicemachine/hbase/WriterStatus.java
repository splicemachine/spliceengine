package com.splicemachine.hbase;

import javax.management.MXBean;

/**
 * Status MBean for managing Table writer information.
 *
 * @author Scott Fines
 * Created on: 3/19/13
 */
@MXBean
public interface WriterStatus {

    long getMaxBufferHeapSize();
    void setMaxBufferHeapSize(long newMaxHeapSize);

    int getMaxBufferEntries();
    void setMaxBufferEntries(int newMaxBufferEntries);

    int getMaxFlushesPerBuffer();
    void setMaxFlushesPerBuffer(int newMaxPendingBuffers);

    int getOutstandingCallBuffers();

    int getPendingBufferFlushes();

    int getExecutingBufferFlushes();

    long getTotalBufferFlushes();

    int getRunningWriteThreads();

    long getNumCachedTables();

    int getNumCachedRegions(String tableName);

    long getCacheLastUpdatedTimeStamp();


    boolean getCompressWrites();

    void setCompressWrites(boolean compressWrites);

}
