package com.splicemachine.si.api;

import java.io.Closeable;
import java.io.IOException;

public interface RowAccumulator<Data> extends Closeable {
    public boolean isOfInterest(Data value);
    public boolean accumulate(Data value) throws IOException;
    public boolean isFinished();
    public byte[] result();
    public long getBytesVisited();
    public boolean isCountStar();
    public void reset();
}
