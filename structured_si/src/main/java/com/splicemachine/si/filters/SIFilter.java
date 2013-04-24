package com.splicemachine.si.filters;

import com.splicemachine.si.api.PutLog;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.SiTransactionId;
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
    private PutLog putLog;
    protected long startTimestamp;

    private FilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(Transactor transactor, PutLog putLog, TransactionId transactionId) throws IOException {
        this.transactor = transactor;
        this.putLog = putLog;
        this.startTimestamp = transactionId.getId();
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
            filterState = transactor.newFilterState(putLog, new SiTransactionId(startTimestamp));
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
