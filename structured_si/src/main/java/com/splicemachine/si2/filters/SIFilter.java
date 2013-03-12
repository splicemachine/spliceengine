package com.splicemachine.si2.filters;

import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.SiTransactionId;
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
    protected STable region;

    private FilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(Transactor transactor, long startTimestamp, STable region) throws IOException {
        this.transactor = transactor;
        this.startTimestamp = startTimestamp;
        this.region = region;
        transactor.newFilterState(region, new SiTransactionId(startTimestamp));
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        try {
            initFilterStateIfNeeded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return transactor.filterKeyValue(filterState, keyValue);
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = transactor.newFilterState(region, new SiTransactionId(startTimestamp));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Thread.dumpStack();
        startTimestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Thread.dumpStack();
        out.writeLong(startTimestamp);
    }
}
