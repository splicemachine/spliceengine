package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 * Created on: 8/28/13
 */
public interface BufferConfiguration {

    long getMaxHeapSize();

    int getMaxEntries();
}
