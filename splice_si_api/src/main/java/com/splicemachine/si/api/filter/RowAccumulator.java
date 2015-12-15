package com.splicemachine.si.api.filter;

import com.splicemachine.storage.DataCell;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator<Data> extends Closeable {
    boolean isOfInterest(Data value);
    boolean isInteresting(DataCell value);
    boolean accumulate(Data value) throws IOException;
    boolean accumulateCell(DataCell value) throws IOException;
    boolean isFinished();
    byte[] result();
    long getBytesVisited();
    boolean isCountStar();
    void reset();
}
