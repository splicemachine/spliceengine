package com.splicemachine.si.api.filter;

import com.splicemachine.storage.DataCell;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator extends Closeable {
    boolean isInteresting(DataCell value);

    boolean accumulateCell(DataCell value) throws IOException;
    boolean isFinished();
    byte[] result();
    long getBytesVisited();
    boolean isCountStar();
    void reset();
}
