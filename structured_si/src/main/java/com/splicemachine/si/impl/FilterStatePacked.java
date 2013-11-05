package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FilterStatePacked<Data, Result, KeyValue, OperationWithAttributes, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes,
        Scan, Lock, OperationStatus, Hashable extends Comparable, Mutation, IHTable, Scanner>
        implements IFilterState<KeyValue> {

    static final Logger LOG = Logger.getLogger(FilterStatePacked.class);

    private final String tableName;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final DataStore dataStore;
    private final FilterState<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus,
            Hashable, Mutation, IHTable, Scanner> simpleFilter;
    private final RowAccumulator<Data> accumulator;
    private KeyValue accumulatedKeyValue = null;
    private boolean hasAccumulation = false;
    private boolean excludeRow = false;
    private boolean siNullSkip = false;

    public FilterStatePacked(String tableName, SDataLib dataLib, DataStore dataStore,
                             FilterState<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock,
                                     OperationStatus, Hashable, Mutation, IHTable, Scanner> simpleFilter,
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
                if (dataStore.isSINull(simpleFilter.keyValue.keyValue())) {
                	final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
                    switch (returnCode) {
                        case INCLUDE:
                        case INCLUDE_AND_NEXT_COL:
                        	siNullSkip = true;
                            return Filter.ReturnCode.NEXT_COL;
                        case SKIP:
                        case NEXT_COL:
                        case NEXT_ROW:
                            return Filter.ReturnCode.SKIP;
                        default:
                            throw new RuntimeException("unknown return code");
                    }
                } else if (accumulator.isOfInterest(simpleFilter.keyValue.value()) && !siNullSkip) { // This behaves similar to a seek next col without the reseek penalty - JL
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
        accumulatedKeyValue = simpleFilter.keyValue.keyValue();
    }

    private Filter.ReturnCode accumulateUserData(KeyValue dataKeyValue) throws IOException {
        final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
        switch (returnCode) {
            case INCLUDE:
            case INCLUDE_AND_NEXT_COL:
                if (!accumulator.accumulate(simpleFilter.keyValue.value())) {
                    excludeRow = true;
                    return Filter.ReturnCode.NEXT_COL;
                }
                if (hasAccumulation) {
                    if (!dataLib.matchingFamilyKeyValue(accumulatedKeyValue, simpleFilter.keyValue.keyValue()) ||
                            !dataLib.matchingRowKeyValue(accumulatedKeyValue, simpleFilter.keyValue.keyValue()) ||
                            !dataLib.matchingQualifierKeyValue(accumulatedKeyValue, simpleFilter.keyValue.keyValue())) {
                        throw new RuntimeException("key value mis-match");
                    }
                } else {
                    accumulated();
                }
                if (accumulator.isFinished()) {
                    return Filter.ReturnCode.NEXT_COL;
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
                final KeyValue keyValue = dataLib.newKeyValue(accumulatedKeyValue, resultData);
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
        accumulatedKeyValue = null;
        siNullSkip = false;
    }

}
