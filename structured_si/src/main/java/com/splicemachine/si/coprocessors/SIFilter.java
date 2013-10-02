package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
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

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilter.class);
    private Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor = null;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private boolean includeSIColumn;

    private IFilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor,
                    TransactionId transactionId, RollForwardQueue rollForwardQueue, boolean includeSIColumn) throws IOException {
        this.transactor = transactor;
        this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.includeSIColumn = includeSIColumn;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        }
        try {
            initFilterStateIfNeeded();
            return transactor.filterKeyValue(filterState, keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = transactor.newFilterState(rollForwardQueue, transactor.transactionIdFromString(transactionIdString),
                    includeSIColumn);
        }
    }

    @Override
    public boolean filterRow() {
        return super.filterRow();
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
