package com.splicemachine.si.impl;

import java.io.IOException;

public interface RowAccumulator<Data, KeyValue> {
    boolean isOfInterest(KeyValue value);
    boolean accumulate(KeyValue value) throws IOException;
    boolean isFinished();
    Data result();
}
