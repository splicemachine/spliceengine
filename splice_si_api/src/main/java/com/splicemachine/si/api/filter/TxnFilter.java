package com.splicemachine.si.api.filter;

import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.api.txn.KeyValueType;

import java.io.IOException;

public interface TxnFilter<Data,ReturnCode> {
    ReturnCode filterKeyValue(Data keyValue) throws IOException;
    void nextRow();
    Data produceAccumulatedKeyValue();
    boolean getExcludeRow();
	KeyValueType getType(Data keyValue) throws IOException;
	DataStore getDataStore();
    RowAccumulator<Data> getAccumulator();
}
