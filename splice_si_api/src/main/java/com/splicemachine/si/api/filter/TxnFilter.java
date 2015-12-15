package com.splicemachine.si.api.filter;

import com.splicemachine.si.impl.DataStore;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

public interface TxnFilter<Data,ReturnCode> extends DataFilter{
    ReturnCode filterKeyValue(Data keyValue) throws IOException;
    void nextRow();
    Data produceAccumulatedKeyValue();

    DataCell produceAccumulatedResult();
    boolean getExcludeRow();
	CellType getType(Data keyValue) throws IOException;
	DataStore getDataStore();
    RowAccumulator<Data> getAccumulator();
}
