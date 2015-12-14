package com.splicemachine.si.impl;

import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.filter.BaseSIFilterPacked;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase {
        BaseSIFilterPacked<Cell,Filter,Get,Result,ReturnCode,Scan> baseSIFilterPacked;

		public SIFilterPacked() {
			super();
		}

		public SIFilterPacked(TxnFilter<Cell,ReturnCode> filterState){
            baseSIFilterPacked = new BaseSIFilterPacked<>(filterState);
		}

		public SIFilterPacked(Txn txn,
													ReadResolver resolver,
													EntryPredicateFilter predicateFilter,
													boolean countStar) throws IOException {
            baseSIFilterPacked = new BaseSIFilterPacked(txn,resolver,predicateFilter,countStar);
		}

		@Override
		public Filter.ReturnCode filterKeyValue(Cell keyValue) {
				return baseSIFilterPacked.internalFilter(keyValue);
		}

		@Override
	    public void filterRowCells(List<Cell> keyValues) {
	        // FIXME: this is scary
	        try {
                baseSIFilterPacked.initFilterStateIfNeeded();
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	        if (!baseSIFilterPacked.filterRow())
	            keyValues.remove(0);
	        final Cell accumulatedValue = baseSIFilterPacked.filterState.produceAccumulatedKeyValue();
	        if (accumulatedValue != null) {
	            keyValues.add(accumulatedValue);
	        }
	    }

}