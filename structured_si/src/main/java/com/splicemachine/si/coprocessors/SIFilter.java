package com.splicemachine.si.coprocessors;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends FilterBase {
    private static Logger LOG = Logger.getLogger(SIFilter.class);
	private TransactionManager transactionManager;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private IFilterState filterState = null;
	private TransactionReadController<Mutation,Get,Scan> readController;
	public SIFilter() {}

    public SIFilter(TransactionReadController<Mutation,Get, Scan> readController,
										TransactionId transactionId, TransactionManager transactionManager, RollForwardQueue rollForwardQueue) throws IOException {
				this.transactionManager = transactionManager;
				this.transactionIdString = transactionId.getTransactionIdString();
				this.rollForwardQueue = rollForwardQueue;
				this.readController = readController;
    }

		@Override
		@SuppressWarnings("unchecked")
    public ReturnCode filterKeyValue(Cell keyValue) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "filterCell %s", keyValue);
        }
        try {
            initFilterStateIfNeeded();
						return filterState.filterCell(keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = readController.newFilterState(rollForwardQueue, transactionManager.transactionIdFromString(transactionIdString));
        }
    }

    @Override
    public boolean filterRow() throws IOException {
        return super.filterRow();
    }

    @Override
    public void reset() {
        if (filterState != null) {
						filterState.nextRow();
        }
    }
}
