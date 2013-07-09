package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FilterStatePacked<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> {
    static final Logger LOG = Logger.getLogger(FilterStatePacked.class);

    private final DataStore dataStore;
    private final FilterState<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> simpleFilter;
    private final RowAccumulator<Data> accumulator;
    private boolean hasAccumulation = false;

    public FilterStatePacked(DataStore dataStore,
                             FilterState<Data, Result, KeyValue, Put, Delete, Get, Scan, OperationWithAttributes, Lock> simpleFilter,
                             RowAccumulator<Data> accumulator) {
        this.dataStore = dataStore;
        this.simpleFilter = simpleFilter;
        this.accumulator = accumulator;
    }

    Filter.ReturnCode filterKeyValue(KeyValue dataKeyValue) throws IOException {
        simpleFilter.setKeyValue(dataKeyValue);
        switch (simpleFilter.type) {
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case COMMIT_TIMESTAMP:
            case OTHER:
                return simpleFilter.filterByColumnType();
            case USER_DATA:
                if (dataStore.isSINull(simpleFilter.keyValue.value())) {
                    return Filter.ReturnCode.NEXT_ROW;
                } else if (accumulator.isOfInterest(simpleFilter.keyValue.value())) {
                    return accumulateUserData(dataKeyValue);
                } else {
                    return Filter.ReturnCode.SKIP;
                }
            default:
                throw new RuntimeException("unknown key value type");
        }
    }

    private Filter.ReturnCode accumulateUserData(KeyValue dataKeyValue) throws IOException {
        final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
        switch (returnCode) {
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
                accumulator.accumulate(simpleFilter.keyValue.value());
                hasAccumulation = true;
                if (accumulator.isFinished()) {
                    return Filter.ReturnCode.NEXT_ROW;
                }
                return Filter.ReturnCode.SKIP;
            case SKIP:
            case NEXT_COL:
            case NEXT_ROW:
                return Filter.ReturnCode.SKIP;
            default:
                throw new RuntimeException("unknown return code");
        }
    }

    public Data produceAccumulatedKeyValue() {
        if (hasAccumulation) {
            return accumulator.result();
        } else {
            return null;
        }
    }

    void nextRow() {
        simpleFilter.nextRow();
        hasAccumulation = false;
    }

}
