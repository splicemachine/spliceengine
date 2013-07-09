package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.HRowLock;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
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
    private boolean includeSIColumn;
    private boolean includeUncommittedAsOfStart;

    private IFilterState<KeyValue> filterState = null;

    public SIFilterPacked() {
    }

    public SIFilterPacked(Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer, HRowLock> transactor,
                    TransactionId transactionId, RollForwardQueue rollForwardQueue,
                    boolean includeSIColumn, boolean includeUncommittedAsOfStart) throws IOException {
        this.transactor = transactor;
        this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.includeSIColumn = includeSIColumn;
        this.includeUncommittedAsOfStart = includeUncommittedAsOfStart;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        try {
            initFilterStateIfNeeded();
            return transactor.filterKeyValue(filterState, keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = transactor.newFilterStatePacked(rollForwardQueue, transactor.transactionIdFromString(transactionIdString),
                    includeSIColumn, includeUncommittedAsOfStart);
        }
    }

    @Override
    public boolean filterRow() {
        return super.filterRow();
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRow(List<KeyValue> keyValues) {
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
