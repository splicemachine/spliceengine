package com.splicemachine.hbase.writer;

import javax.management.MXBean;

/**
 * Status MBean for managing Table writer information.
 *
 * @author Scott Fines
 * Created on: 3/19/13
 */
@MXBean
public interface WriterStatus {

    int getExecutingBufferFlushes();

    long getTotalSubmittedFlushes();

    long getFailedBufferFlushes();

    long getNotServingRegionFlushes();

    long getWrongRegionFlushes();

    long getTimedOutFlushes();

    long getGlobalErrors();

    long getPartialFailures();

    long getMaxFlushTime();

    long getMinFlushTime();

    long getTotalFlushTime();

    long getMaxFlushedBufferSize();

    long getTotalFlushedBufferSize();

    double getAvgFlushedBufferSize();

    long getMinFlushedBufferSize();

    double getAvgFlushTime();

    long getMaxFlushedBufferEntries();

    long getTotalFlushedBufferEntries();

    double getAvgFlushedBufferEntries();

    long getMinFlushedBufferEntries();

    void reset();

		long getTotalRejectedFlushes();


}
