package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.splicemachine.si.api.RollForwardQueue;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.kryo.KryoPool;
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
		private final TransactionStore transactionStore;
		private final TransactionManager control;

		public SITransactionReadController(DataStore dataStore,
																			 SDataLib dataLib,
																			 TransactionStore transactionStore,
																			 TransactionManager control) {
				this.dataStore = dataStore;
				this.dataLib = dataLib;
				this.transactionStore = transactionStore;
				this.control = control;
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
		public IFilterState newFilterState(TransactionId transactionId) throws IOException {
				return newFilterState(null, transactionId);
		}

		@Override
		public IFilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId) throws IOException {
				return new FilterState(dataLib, dataStore, transactionStore, rollForwardQueue, 
								transactionStore.getImmutableTransaction(transactionId));
		}

		@Override
		@SuppressWarnings("unchecked")
		public IFilterState newFilterStatePacked(String tableName, RollForwardQueue rollForwardQueue, EntryPredicateFilter predicateFilter, TransactionId transactionId, boolean countStar) throws IOException {
				return new FilterStatePacked(
								(FilterState) newFilterState(rollForwardQueue, transactionId),
								new HRowAccumulator(predicateFilter, new EntryDecoder(KryoPool.defaultPool()), countStar ));
		}

		@Override
		@SuppressWarnings("unchecked")
		public Filter.ReturnCode filterKeyValue(IFilterState filterState, KeyValue keyValue) throws IOException {
				return filterState.filterKeyValue(keyValue);
		}

		@Override
		public void filterNextRow(IFilterState filterState) {
				filterState.nextRow();
		}

		@Override
		@SuppressWarnings("unchecked")
		public Result filterResult(IFilterState filterState, Result result) throws IOException {
				//TODO -sf- this is only used in testing--ignore when production tuning
				final SDataLib<Put, Delete, Get, Scan> dataLib = dataStore.dataLib;
				final List<KeyValue> filteredCells = Lists.newArrayList();
				final List<KeyValue> KVs = dataLib.listResult(result);
				if (KVs != null) {
						byte[] currentRowKey = null;
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
		public DDLFilter newDDLFilter(String parentTransactionId, String transactionId) throws IOException {
				return new DDLFilter(
								transactionStore.getTransaction(control.transactionIdFromString(transactionId)),
								parentTransactionId == null ? null : transactionStore.getTransaction(control.transactionIdFromString(parentTransactionId)),
								transactionStore
				);
		}


}
