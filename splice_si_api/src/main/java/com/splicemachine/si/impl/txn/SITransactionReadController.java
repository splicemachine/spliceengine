package com.splicemachine.si.impl.txn;

import com.google.common.collect.Lists;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController<OperationWithAttributes,Data,
                Delete extends OperationWithAttributes,
                Filter,
				Get extends OperationWithAttributes,
                Put extends OperationWithAttributes,
                Mutation,
                OperationStatus,
                RegionScanner,
                Result,
                ReturnCode,
                RowLock,
				Scan extends OperationWithAttributes,Table>
				implements TransactionReadController<Data,Filter,Get,Result,ReturnCode,Scan> {
		private final DataStore<OperationWithAttributes,Data,Delete,Filter,
                        Get,Mutation,OperationStatus,
                        Put,RegionScanner,Result,ReturnCode,RowLock,Scan,Table> dataStore = SIDriver.getDataStore();
		private final SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
                Put,RegionScanner,Result,Scan> dataLib = SIDriver.getDataLib();
		private final TxnSupplier txnSupplier = SIDriver.getTxnSupplier();
        private final IgnoreTxnCacheSupplier<OperationWithAttributes,Data,Delete,Filter,Get,
                Put,RegionScanner,Result,Scan,Table> ignoreTxnCacheSupplier = SIDriver.getIgnoreTxnCacheSupplier();

        public SITransactionReadController() {
		}

		@Override
		public boolean isFilterNeededGet(Get get) {
				return isFlaggedForSITreatment(get)
								&& !dataStore.isSuppressIndexing(get);
		}

		private boolean isFlaggedForSITreatment(OperationWithAttributes op) {
				return dataStore.getSINeededAttribute(op)!=null;
		}

		@Override
		public boolean isFilterNeededScan(Scan scan) {
				return isFlaggedForSITreatment(scan)
								&& !dataStore.isSuppressIndexing(scan);
		}

		@Override
		public void preProcessGet(Get get) throws IOException {
				dataLib.setGetTimeRange(get, 0, Long.MAX_VALUE);
				dataLib.setGetMaxVersions(get);
		}

		@Override
		public void preProcessScan(Scan scan) throws IOException {
				dataLib.setScanTimeRange(scan, 0, Long.MAX_VALUE);
				dataLib.setScanMaxVersions(scan);
		}

		@Override
		public TxnFilter newFilterState(Txn txn) throws IOException {
				return newFilterState(null,txn);
		}

		@Override
		public TxnFilter newFilterState(ReadResolver readResolver, Txn txn) throws IOException {
				return new SimpleTxnFilter(null,txn,readResolver);
		}

		@Override
		public TxnFilter newFilterStatePacked(ReadResolver readResolver,
																						 EntryPredicateFilter predicateFilter, Txn txn, boolean countStar) throws IOException {
			return new PackedTxnFilter(newFilterState(txn),
					SIDriver.siFactory.getRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
		}

		@Override
		public ReturnCode filterKeyValue(TxnFilter<Data,ReturnCode> filterState, Data data) throws IOException {
				return filterState.filterKeyValue(data);
		}

        @Override
		public void filterNextRow(TxnFilter filterState) {
				filterState.nextRow();
		}

		@Override
		public Result filterResult(TxnFilter<Data,ReturnCode> filterState, Result result) throws IOException {
				//TODO -sf- this is only used in testing--ignore when production tuning
				final List<Data> filteredCells = Lists.newArrayList();
				final List<Data> KVs = dataLib.listResult(result);
				if (KVs != null) {
						for (Data kv : KVs) {
							filterKeyValue(filterState, kv);
						}
						if (!filterState.getExcludeRow())
							filteredCells.add(filterState.produceAccumulatedKeyValue());
				}
				if (filteredCells.isEmpty()) {
					return null;
				} else {
					return dataStore.dataLib.newResult(filteredCells);
				}
		}

		@Override
		public DDLFilter newDDLFilter(TxnView txn) throws IOException {
        return new DDLFilter(txn);
		}


}
