package com.splicemachine.si.impl;

import java.io.IOException;

public interface RowAccumulator<Data> {
    boolean isOfInterest(Data value);
    boolean accumulate(Data value) throws IOException;
    boolean isFinished();
    Data result();
}
