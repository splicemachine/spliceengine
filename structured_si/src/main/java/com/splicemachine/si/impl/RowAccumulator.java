package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;

public interface RowAccumulator {
    boolean isOfInterest(KeyValue value);
    boolean accumulate(KeyValue value) throws IOException;
    boolean isFinished();
    byte[] result();

		long getBytesVisited();
}
