package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.FilterState;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.RollForwardQueue;
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
    private Transactor transactor = null;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private boolean siOnly;

    private FilterState filterState = null;

    public SIFilter() {
    }

    public SIFilter(Transactor transactor, TransactionId transactionId, RollForwardQueue rollForwardQueue, boolean siOnly) throws IOException {
        this.transactor = transactor;
        this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.siOnly = siOnly;
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
            filterState = transactor.newFilterState(rollForwardQueue, transactor.transactionIdFromString(transactionIdString), siOnly);
        }
    }

    @Override
    public boolean filterRow() {
        return super.filterRow();    //To change body of overridden methods use File | Settings | File Templates.
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
