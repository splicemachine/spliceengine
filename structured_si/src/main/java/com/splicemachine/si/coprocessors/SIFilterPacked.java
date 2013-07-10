package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.HRowLock;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
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
    private Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer, HRowLock> transactor = null;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private EntryPredicateFilter predicateFilter;
    private boolean includeSIColumn;
    private boolean includeUncommittedAsOfStart;

    // always include at least one keyValue so that we can use the "hook" of filterRow(...) to generate the accumulated key value
    private Boolean extraKeyValueIncluded = null;

    private IFilterState<KeyValue> filterState = null;

    public SIFilterPacked() {
    }

    public SIFilterPacked(Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer, HRowLock> transactor,
                          TransactionId transactionId, RollForwardQueue rollForwardQueue, EntryPredicateFilter predicateFilter,
                          boolean includeSIColumn, boolean includeUncommittedAsOfStart) throws IOException {
        this.transactor = transactor;
        this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.predicateFilter = predicateFilter;
        this.includeSIColumn = includeSIColumn;
        this.includeUncommittedAsOfStart = includeUncommittedAsOfStart;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
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
            }
            return returnCode;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = transactor.newFilterStatePacked(rollForwardQueue, predicateFilter,
                    transactor.transactionIdFromString(transactionIdString), includeSIColumn, includeUncommittedAsOfStart);
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
        transactionIdString = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(transactionIdString);
    }
}