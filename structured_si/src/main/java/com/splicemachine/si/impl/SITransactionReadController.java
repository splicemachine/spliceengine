package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController<
				Get extends OperationWithAttributes,
				Scan extends OperationWithAttributes,
				Delete extends OperationWithAttributes,
				Put extends OperationWithAttributes
				>
				implements TransactionReadController<Get,Scan>{
		private final DataStore dataStore;
		private final SDataLib dataLib;
		private final TxnSupplier txnSupplier;


    public SITransactionReadController(DataStore dataStore,
                                       SDataLib dataLib,
                                       TxnSupplier txnSupplier) {
				this.dataStore = dataStore;
				this.dataLib = dataLib;
				this.txnSupplier = txnSupplier;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isFilterNeededGet(Get get) {
				return isFlaggedForSITreatment(get)
								&& !dataStore.isSuppressIndexing(get);
		}

		@SuppressWarnings("unchecked")
		private boolean isFlaggedForSITreatment(OperationWithAttributes op) {
				return dataStore.getSINeededAttribute(op)!=null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isFilterNeededScan(Scan scan) {
				return isFlaggedForSITreatment(scan)
								&& !dataStore.isSuppressIndexing(scan);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void preProcessGet(Get get) throws IOException {
				dataLib.setGetTimeRange(get, 0, Long.MAX_VALUE);
				dataLib.setGetMaxVersions(get);
		}

		@Override
		@SuppressWarnings("unchecked")
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
				return new SimpleTxnFilter(txnSupplier,txn,readResolver,dataStore);
		}

		@Override
		public TxnFilter newFilterStatePacked(ReadResolver readResolver,
																						 EntryPredicateFilter predicateFilter, Txn txn, boolean countStar) throws IOException {
			return new PackedTxnFilter(newFilterState(txn),
					SIFactoryDriver.siFactory.getRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
		}

		@Override
		@SuppressWarnings("unchecked")
		public Filter.ReturnCode filterKeyValue(TxnFilter filterState, KeyValue keyValue) throws IOException {
				return filterState.filterKeyValue(keyValue);
		}

		@Override
		public void filterNextRow(TxnFilter filterState) {
				filterState.nextRow();
		}

		@Override
		@SuppressWarnings("unchecked")
		public Result filterResult(TxnFilter filterState, Result result) throws IOException {
				//TODO -sf- this is only used in testing--ignore when production tuning
				final SDataLib<Put, Delete, Get, Scan> dataLib = dataStore.dataLib;
				final List<KeyValue> filteredCells = Lists.newArrayList();
				final List<KeyValue> KVs = dataLib.listResult(result);
				if (KVs != null) {
//						byte[] currentRowKey = null;
						for (KeyValue kv : KVs) {
							filterKeyValue(filterState, kv);
						}
						if (!filterState.getExcludeRow())
							filteredCells.add(filterState.produceAccumulatedKeyValue());
				}
				if (filteredCells.isEmpty()) {
					return null;
				} else {
					return new Result(filteredCells);
				}
		}

		@Override
		public DDLFilter newDDLFilter(TxnView txn) throws IOException {
        return new DDLFilter(txn);
		}


}
