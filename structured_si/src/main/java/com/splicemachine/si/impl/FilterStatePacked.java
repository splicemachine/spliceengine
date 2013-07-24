package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FilterStatePacked<Data, Result, KeyValue, OperationWithAttributes, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes,
        Scan, Lock, OperationStatus, Hashable, Mutation, IHTable>
        implements IFilterState<KeyValue> {

    static final Logger LOG = Logger.getLogger(FilterStatePacked.class);

    private final String tableName;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final DataStore dataStore;
    private final FilterState<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus,
            Hashable, Mutation, IHTable> simpleFilter;
    private final RowAccumulator<Data> accumulator;
    private Data qualifier = null;
    private Data family = null;
    private Data rowKey = null;
    private Long timestamp = null;
    private boolean hasAccumulation = false;
    private boolean excludeRow = false;

    public FilterStatePacked(String tableName, SDataLib dataLib, DataStore dataStore,
                             FilterState<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock,
                                     OperationStatus, Hashable, Mutation, IHTable> simpleFilter,
                             RowAccumulator<Data> accumulator) {
        this.tableName = tableName;
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.simpleFilter = simpleFilter;
        this.accumulator = accumulator;
        simpleFilter.setIgnoreDoneWithColumn();
    }

    @Override
    public Filter.ReturnCode filterKeyValue(KeyValue dataKeyValue) throws IOException {
        simpleFilter.setKeyValue(dataKeyValue);
        switch (simpleFilter.type) {
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case COMMIT_TIMESTAMP:
            case OTHER:
                return simpleFilter.filterByColumnType();
            case USER_DATA:
                if (dataStore.isSINull(simpleFilter.keyValue.value())) {
                    final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
                    switch (returnCode) {
                        case INCLUDE:
                        case INCLUDE_AND_NEXT_COL:
                            return Filter.ReturnCode.NEXT_ROW;
                        case SKIP:
                        case NEXT_COL:
                        case NEXT_ROW:
                            return Filter.ReturnCode.SKIP;
                        default:
                            throw new RuntimeException("unknown return code");
                    }
                } else if (accumulator.isOfInterest(simpleFilter.keyValue.value())) {
                    return accumulateUserData(dataKeyValue);
                } else {
                    if (!hasAccumulation) {
                        final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
                        switch (returnCode) {
                            case INCLUDE:
                            case INCLUDE_AND_NEXT_COL:
                                accumulated();
                                break;
                        }
                    }
                    return Filter.ReturnCode.SKIP;
                }
            default:
                throw new RuntimeException("unknown key value type");
        }
    }

    private void accumulated() {
        hasAccumulation = true;
        family = simpleFilter.keyValue.family();
        qualifier = simpleFilter.keyValue.qualifier();
        timestamp = simpleFilter.keyValue.timestamp();
        rowKey = simpleFilter.keyValue.row();
    }

    private Filter.ReturnCode accumulateUserData(KeyValue dataKeyValue) throws IOException {
        final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
        switch (returnCode) {
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
                if (!accumulator.accumulate(simpleFilter.keyValue.value())) {
                    excludeRow = true;
                    return Filter.ReturnCode.NEXT_ROW;
                }
                if (hasAccumulation) {
                    if (!dataLib.valuesEqual(family, simpleFilter.keyValue.family()) ||
                            !dataLib.valuesEqual(rowKey, simpleFilter.keyValue.row()) ||
                            !dataLib.valuesEqual(qualifier, simpleFilter.keyValue.qualifier())) {
                        throw new RuntimeException("key value mis-match");
                    }
                } else {
                    accumulated();
                }
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

    @Override
    public KeyValue produceAccumulatedKeyValue() {
        if (hasAccumulation) {
            final Data resultData = accumulator.result();
            if (resultData != null) {
                final KeyValue keyValue = dataLib.newKeyValue(rowKey, family, qualifier, timestamp, resultData);
                return keyValue;
            } else {
                excludeRow = true;
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean getExcludeRow() {
        return excludeRow;
    }

    @Override
    public void nextRow() {
        simpleFilter.nextRow();
        hasAccumulation = false;
        excludeRow = false;
        qualifier = null;
        family = null;
        rowKey = null;
        timestamp = null;
    }

}
