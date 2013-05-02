package com.splicemachine.si.filters;

import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SITransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SIFilter extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilter.class);
    private Transactor transactor = null;
    protected long startTimestamp;
    protected RollForwardQueue rollForwardQueue;

    private FilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(Transactor transactor, TransactionId transactionId, RollForwardQueue rollForwardQueue) throws IOException {
        this.transactor = transactor;
        this.startTimestamp = transactionId.getId();
        this.rollForwardQueue = rollForwardQueue;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
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
            filterState = transactor.newFilterState(rollForwardQueue, new SITransactionId(startTimestamp));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startTimestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(startTimestamp);
    }
}
