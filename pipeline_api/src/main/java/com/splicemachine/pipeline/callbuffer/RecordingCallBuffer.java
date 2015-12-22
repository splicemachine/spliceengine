package com.splicemachine.pipeline.callbuffer;


import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.callbuffer.CallBuffer;

/**
 * Recording Call Buffer
 * 
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

	WriteStats getWriteStats();
}
