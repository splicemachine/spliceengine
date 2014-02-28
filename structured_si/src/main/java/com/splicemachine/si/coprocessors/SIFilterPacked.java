package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase implements HasPredicateFilter {
    private static Logger LOG = Logger.getLogger(SIFilterPacked.class);
    private String tableName;
	private TransactionReadController<Get,Scan> readController;
	private TransactionManager transactionManager;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private EntryPredicateFilter predicateFilter;

    // always include at least one keyValue so that we can use the "hook" of filterRow(...) to generate the accumulated key value
    private Boolean extraKeyValueIncluded = null;

    private IFilterState filterState = null;

    public SIFilterPacked() {
    }

    public SIFilterPacked(String tableName,
													TransactionId transactionId,
													TransactionManager transactionManager,
													RollForwardQueue rollForwardQueue,
													EntryPredicateFilter predicateFilter,
													TransactionReadController<Get, Scan> readController) throws IOException {
        this.tableName = tableName;
		this.transactionManager = transactionManager;
		this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.predicateFilter = predicateFilter;
		this.readController = readController;
 	}

		@Override
		public long getBytesVisited(){
				if(filterState==null) return 0l;
				FilterStatePacked packed = (FilterStatePacked)filterState;
				@SuppressWarnings("unchecked") RowAccumulator accumulator = packed.getAccumulator();
				return accumulator.getBytesVisited();
		}

		@Override
		public EntryPredicateFilter getFilter(){
				return predicateFilter;
		}

    @Override
    public ReturnCode filterKeyValue(KeyValue keyValue) {
        try {
            initFilterStateIfNeeded();
            return filterState.filterKeyValue(keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
		private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = readController.newFilterStatePacked(tableName, rollForwardQueue, predicateFilter,
                    transactionManager.transactionIdFromString(transactionIdString));
        }
    }

    @Override 
    public boolean filterRow() { 
    	return filterState.getExcludeRow(); 	
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
    	if (!filterRow())
    		keyValues.remove(0);
        final KeyValue accumulatedValue = filterState.produceAccumulatedKeyValue();
        if (accumulatedValue != null) {
            keyValues.add(accumulatedValue);
        }
    }

    @Override
    public void reset() {
        extraKeyValueIncluded = null;
        if (filterState != null) {
						filterState.nextRow();
        }
    }

    @Override public void readFields(DataInput in) throws IOException { }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("This filter should not be serialized");
    }
}