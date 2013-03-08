package com.splicemachine.si2.filters;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.data.hbase.HDataLib;
import com.splicemachine.si2.data.hbase.HDataLibAdapter;
import com.splicemachine.si2.data.hbase.HStore;
import com.splicemachine.si2.data.hbase.HTableReaderAdapter;
import com.splicemachine.si2.data.hbase.HTableWriterAdapter;
import com.splicemachine.si2.data.hbase.HbRegion;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.FilterState;
import com.splicemachine.si2.si.api.IdSource;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.RowMetadataStore;
import com.splicemachine.si2.si.impl.SiTransactionId;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SIFilter extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilter.class);
    protected long startTimestamp;
    protected HRegion region;

    private FilterState filterState = null;
    private Transactor transactor = null;

    public SIFilter() {
    }

    /**
     * Server side filter wrapped into other filters on the coprocessor side.
     *
     * @param startTimestamp
     * @param region
     */
    public SIFilter(long startTimestamp, HRegion region) {
        this.startTimestamp = startTimestamp;
        this.region = region;
    }

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        initFilterStateIfNeeded();
        return transactor.filterKeyValue(filterState, keyValue);
    }

    private void initFilterStateIfNeeded() {
        if (filterState == null) {
            transactor = TransactorFactory.newTransactorForFiltering();
            filterState = transactor.newFilterState(new HbRegion(region), new SiTransactionId(startTimestamp));
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
