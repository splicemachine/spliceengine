package com.splicemachine.si.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.Filter;

public interface IFilterState {
    Filter.ReturnCode filterCell(Cell keyValue) throws IOException;
    void nextRow();
    Cell produceAccumulatedCell();
    boolean getExcludeRow();
}
