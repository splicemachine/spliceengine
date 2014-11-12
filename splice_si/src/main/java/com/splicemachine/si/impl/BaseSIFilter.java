package com.splicemachine.si.impl;

import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public abstract class BaseSIFilter<Data> extends FilterBase implements Writable{
    private static Logger LOG = Logger.getLogger(BaseSIFilter.class);
    private TxnFilter filterState = null;

    public BaseSIFilter() {}

    public BaseSIFilter(TxnFilter txnFilter){
        this.filterState = txnFilter;
    }

    public ReturnCode internalFilter(Data keyValue) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        }
        try {
            initFilterStateIfNeeded();
						return filterState.filterKeyValue(keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void initFilterStateIfNeeded() throws IOException {
    }

    @Override
    public boolean filterRow() {
        return filterState.getExcludeRow();
    }

    @Override public boolean hasFilterRow() { return true; }

    @Override
    public void reset() {
        if (filterState != null) {
						filterState.nextRow();
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("This filter should not be serializing");
    }
}
