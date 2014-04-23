package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.KeyValue;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator extends Closeable {
    public boolean isOfInterest(KeyValue value);
    public boolean accumulate(KeyValue value) throws IOException;
    public boolean isFinished();
    public byte[] result();
    public long getBytesVisited();
    public boolean isCountStar();
    public void reset();
}
