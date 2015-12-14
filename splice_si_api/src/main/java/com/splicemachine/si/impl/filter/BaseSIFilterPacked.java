package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class BaseSIFilterPacked<Data,Filter,Get,Result,ReturnCode,Scan> implements HasPredicateFilter {
    private Txn txn;
		private TransactionReadController<Data,Filter,Get,Result,ReturnCode,Scan> readController = SIDriver.getTransactionReadController();
		private EntryPredicateFilter predicateFilter;
		public TxnFilter<Data,ReturnCode> filterState = null;
		private boolean countStar = false;
		private ReadResolver readResolver;

		public BaseSIFilterPacked() {
		}

		public BaseSIFilterPacked(TxnFilter<Data,ReturnCode> filterState){
				this.filterState = filterState;
		}

		public BaseSIFilterPacked(Txn txn,
													ReadResolver resolver,
													EntryPredicateFilter predicateFilter,
													boolean countStar) throws IOException {
				this.txn = txn;
				this.readResolver = resolver;
				this.predicateFilter = predicateFilter;
				this.countStar = countStar;
		}

		@Override
		public long getBytesVisited(){
				if(filterState==null) return 0l;
				RowAccumulator<Data> accumulator = filterState.getAccumulator();
				return accumulator.getBytesVisited();
		}

		@Override
		public EntryPredicateFilter getFilter(){
				return predicateFilter;
		}

		public ReturnCode internalFilter(Data keyValue) {
				try {
						initFilterStateIfNeeded();
						ReturnCode code= filterState.filterKeyValue(keyValue);
						return code;
				} catch (IOException e) {
						throw new RuntimeException(e);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
		}

		public void initFilterStateIfNeeded() throws IOException {
				if (filterState == null) {
						filterState = readController.newFilterStatePacked(readResolver, predicateFilter, txn, countStar);
				}
		}

		public boolean filterRow() {
				return filterState.getExcludeRow();
		}

		public boolean hasFilterRow() {
				return true;
		}

		public void internalFilterRow(List<Data> keyValues) {
				try {
						initFilterStateIfNeeded();
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				if (!filterRow()&&keyValues.size()>0)
						keyValues.remove(0);
				final Data accumulatedValue = filterState.produceAccumulatedKeyValue();
				if (accumulatedValue != null) {
						keyValues.add(accumulatedValue);
				}
		}

		public void reset() {
				if (filterState != null)
						filterState.nextRow();
		}

}