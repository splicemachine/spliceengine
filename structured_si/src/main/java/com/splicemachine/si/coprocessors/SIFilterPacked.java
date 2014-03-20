package com.splicemachine.si.coprocessors;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;

import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase implements HasPredicateFilter {
//    private static Logger LOG = Logger.getLogger(SIFilterPacked.class);
    private String tableName;
	private TransactionReadController<Mutation,Get,Scan> readController;
	private TransactionManager transactionManager;
    protected String transactionIdString;
    protected RollForwardQueue rollForwardQueue;
    private EntryPredicateFilter predicateFilter;
    private IFilterState filterState = null;
    private boolean countStar = false;

    public SIFilterPacked() {
    }

    public SIFilterPacked(String tableName,
													TransactionId transactionId,
													TransactionManager transactionManager,
													RollForwardQueue rollForwardQueue,
													EntryPredicateFilter predicateFilter,
													TransactionReadController<Mutation,Get, Scan> readController, boolean countStar) throws IOException {
        this.tableName = tableName;
		this.transactionManager = transactionManager;
		this.transactionIdString = transactionId.getTransactionIdString();
        this.rollForwardQueue = rollForwardQueue;
        this.predicateFilter = predicateFilter;
		this.readController = readController;
		this.countStar = countStar;
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
    public ReturnCode filterKeyValue(Cell keyValue) {
        try {
            initFilterStateIfNeeded();
            return filterState.filterCell(keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
		private void initFilterStateIfNeeded() throws IOException {
        if (filterState == null) {
            filterState = readController.newFilterStatePacked(tableName, rollForwardQueue, predicateFilter,
                    transactionManager.transactionIdFromString(transactionIdString), countStar);
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
    public void filterRowCells(List<Cell> keyValues) {
        // FIXME: this is scary
        try {
            initFilterStateIfNeeded();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    	if (!filterRow())
    		keyValues.remove(0);
        final Cell accumulatedValue = filterState.produceAccumulatedCell();
        if (accumulatedValue != null) {
            keyValues.add(accumulatedValue);
        }
    }

    @Override
    public void reset() {
        if (filterState != null)
        	filterState.nextRow();
    }
}