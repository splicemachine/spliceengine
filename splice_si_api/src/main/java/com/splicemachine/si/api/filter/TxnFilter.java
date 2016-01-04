package com.splicemachine.si.api.filter;

import com.splicemachine.si.impl.DataStore;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

public interface TxnFilter extends DataFilter{
    void nextRow();

    DataCell produceAccumulatedResult();
    boolean getExcludeRow();

    DataStore getDataStore();
    RowAccumulator getAccumulator();
}
