package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.KeyValue;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator extends Closeable {
    boolean isOfInterest(KeyValue value);
    boolean accumulate(KeyValue value) throws IOException;
    boolean isFinished();
    byte[] result();
		long getBytesVisited();
		boolean isCountStar();

		void reset();
}
