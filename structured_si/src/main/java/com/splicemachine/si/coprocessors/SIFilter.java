package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactor;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilter.class);
    private HTransactor transactor = null;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private boolean includeSIColumn;
    private boolean includeUncommittedAsOfStart;

    private FilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(HTransactor transactor, TransactionId transactionId, RollForwardQueue rollForwardQueue,
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
            filterState = transactor.newFilterState(rollForwardQueue, transactor.transactionIdFromString(transactionIdString),
                    includeSIColumn, includeUncommittedAsOfStart);
        }
    }

    @Override
    public boolean filterRow() {
        return super.filterRow();
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
