package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.Txn;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends BaseSIFilterPacked<Cell> {

		public SIFilterPacked() {
			super();
		}

		public SIFilterPacked(TxnFilter<Cell> filterState){
			super(filterState);
		}

		public SIFilterPacked(Txn txn,
													ReadResolver resolver,
													EntryPredicateFilter predicateFilter,
													TransactionReadController<Cell,Get, Scan> readController,
													boolean countStar) throws IOException {
			super(txn,resolver,predicateFilter,readController,countStar);
		}

		@Override
		public ReturnCode filterKeyValue(Cell keyValue) {
				return internalFilter(keyValue);
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
	        final Cell accumulatedValue = filterState.produceAccumulatedKeyValue();
	        if (accumulatedValue != null) {
	            keyValues.add(accumulatedValue);
	        }
	    }
		

}