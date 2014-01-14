package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilterPacked.class);

    private String tableName;
    private Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = null;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private EntryPredicateFilter predicateFilter;
    private boolean includeSIColumn;

    // always include at least one keyValue so that we can use the "hook" of filterRow(...) to generate the accumulated key value
    private Boolean extraKeyValueIncluded = null;

    private IFilterState<KeyValue> filterState = null;

    public SIFilterPacked() {
    }

    public SIFilterPacked(String tableName, Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor,
                          TransactionId transactionId, RollForwardQueue rollForwardQueue, EntryPredicateFilter predicateFilter,
                          boolean includeSIColumn) throws IOException {
        this.tableName = tableName;
        this.transactor = transactor;
        this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.predicateFilter = predicateFilter;
        this.includeSIColumn = includeSIColumn;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        try {
            initFilterStateIfNeeded();
            ReturnCode returnCode = transactor.filterKeyValue(filterState, keyValue);
            switch (returnCode) {
                case INCLUDE:
                case INCLUDE_AND_NEXT_COL:
                    if (extraKeyValueIncluded == null) {
                        extraKeyValueIncluded = false;
                    }
                    break;
                case SKIP:
                    if (extraKeyValueIncluded == null) {
                        extraKeyValueIncluded = true;
                        returnCode = ReturnCode.INCLUDE;
                    } 
                    break;
                case NEXT_COL:
                	returnCode = ReturnCode.SKIP;
                    break;
          // We are still re-seeking - TODO JL
          //      case NEXT_COL:
          //      	returnCode = ReturnCode.SKIP;
            }
            return returnCode;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = transactor.newFilterStatePacked(tableName, rollForwardQueue, predicateFilter,
                    transactor.transactionIdFromString(transactionIdString), includeSIColumn);
        }
    }

    @Override
    public boolean filterRow() {
        return filterState.getExcludeRow();
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRow(List<KeyValue> keyValues) {
        if (extraKeyValueIncluded != null && extraKeyValueIncluded) {
            keyValues.remove(0);
        }
        try {
            initFilterStateIfNeeded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final KeyValue accumulatedValue = filterState.produceAccumulatedKeyValue();
        if (accumulatedValue != null) {
            keyValues.add(accumulatedValue);
        }
    }

    @Override
    public void reset() {
        extraKeyValueIncluded = null;
        if (filterState != null) {
            transactor.filterNextRow(filterState);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("This filter should not be serialized");
    }
}