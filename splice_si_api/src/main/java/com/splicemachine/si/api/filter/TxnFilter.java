package com.splicemachine.si.api.filter;

import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

public interface TxnFilter extends DataFilter{
    void nextRow();

    DataCell produceAccumulatedResult();
    boolean getExcludeRow();

    RowAccumulator getAccumulator();
}
