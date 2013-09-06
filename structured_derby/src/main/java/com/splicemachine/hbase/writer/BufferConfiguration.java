package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 * Created on: 8/28/13
 */
public interface BufferConfiguration {

    /**
     * @return the maximum heap space to allow the buffer to occupy before flushing
     */
    long getMaxHeapSize();

    /**
     * @return the maximum number of records to buffer before flushing
     */
    int getMaxEntries();

    /**
     * Note: Not all buffers are required to abide by this setting (if, for example, the buffer
     * does not do any per-region grouping, it will not obey this condition).
     *
     * @return the maximum number of concurrent flushes that are allowed for a given region.
     */
    int getMaxFlushesPerRegion();
}
