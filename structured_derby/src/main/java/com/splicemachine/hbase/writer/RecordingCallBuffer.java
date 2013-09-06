package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public interface RecordingCallBuffer<E> extends CallBuffer<E>{
    long getTotalElementsAdded();

    long getTotalBytesAdded();

    long getTotalFlushes();

    double getAverageEntriesPerFlush();

    double getAverageSizePerFlush();

    CallBuffer<E> unwrap();
}
