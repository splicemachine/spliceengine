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
import org.apache.hadoop.hbase.util.Bytes;

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
		public boolean isGetIncludeSIColumn(Get get) {
				return dataStore.isIncludeSIColumn(get);
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean isScanIncludeSIColumn(Scan scan) {
				return dataStore.isIncludeSIColumn(scan);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void preProcessGet(Get get) throws IOException {
				dataLib.setGetTimeRange(get, 0, Long.MAX_VALUE);
				dataLib.setGetMaxVersions(get);
				if (dataStore.isIncludeSIColumn(get)) {
						dataStore.addSIFamilyToGet(get);
				} else {
						dataStore.addSIFamilyToGetIfNeeded(get);
				}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void preProcessScan(Scan scan) throws IOException {
				dataLib.setScanTimeRange(scan, 0, Long.MAX_VALUE);
				dataLib.setScanMaxVersions(scan);
				if (dataStore.isIncludeSIColumn(scan)) {
						dataStore.addSIFamilyToScan(scan);
				} else {
						dataStore.addSIFamilyToScanIfNeeded(scan);
				}
		}

		@Override
		public IFilterState newFilterState(TransactionId transactionId) throws IOException {
				return newFilterState(null, transactionId, false);
		}

		@Override
		public IFilterState newFilterState(RollForwardQueue rollForwardQueue, TransactionId transactionId, boolean includeSIColumn) throws IOException {
				return new FilterState(dataLib, dataStore, transactionStore, rollForwardQueue, includeSIColumn,
								transactionStore.getImmutableTransaction(transactionId));
		}

		@Override
		@SuppressWarnings("unchecked")
		public IFilterState newFilterStatePacked(String tableName, RollForwardQueue rollForwardQueue, EntryPredicateFilter predicateFilter, TransactionId transactionId, boolean includeSIColumn) throws IOException {
				return new FilterStatePacked(tableName, dataLib, dataStore,
								(FilterState) newFilterState(rollForwardQueue, transactionId, includeSIColumn),
								new HRowAccumulator(predicateFilter, new EntryDecoder(KryoPool.defaultPool())));
		}

		@Override
		@SuppressWarnings("unchecked")
		public Filter.ReturnCode filterKeyValue(IFilterState filterState, KeyValue keyValue) throws IOException {
				//TODO -sf- excessive method, remove
				return filterState.filterKeyValue(keyValue);
		}

		@Override
		public void filterNextRow(IFilterState filterState) {
				//TODO -sf- excessive method, remove
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
						byte[] qualifierToSkip = null;
						byte[] familyToSkip = null;
						byte[] currentRowKey = null;
						for (KeyValue kv : KVs) {
								final byte[] rowKey = kv.getRow();
								if (currentRowKey == null || !Bytes.equals(currentRowKey, rowKey)) {
										currentRowKey = rowKey;
										filterState.nextRow();
								}
								//noinspection StatementWithEmptyBody
								if (familyToSkip != null
												&&kv.matchingColumn(familyToSkip,qualifierToSkip)){
//												&& Bytes.equals(familyToSkip,dataLib.getKeyValueFamily(kv))
//												&& dataLib.valuesEqual(familyToSkip, dataLib.getKeyValueFamily(KV))
//												&& Bytes.equals(qualifierToSkip, dataLib.getKeyValueQualifier(kv))){
//												&& dataLib.valuesEqual(qualifierToSkip, dataLib.getKeyValueQualifier(KV))) {
										// skipping to next column
								} else {
										familyToSkip = null;
										qualifierToSkip = null;
										boolean nextRow = false;
										Filter.ReturnCode returnCode = filterKeyValue(filterState, kv);
										switch (returnCode) {
												case SKIP:
														break;
												case INCLUDE:
														filteredCells.add(kv);
														break;
												case NEXT_COL:
														qualifierToSkip = kv.getQualifier();
														familyToSkip = kv.getFamily();
														break;
												case NEXT_ROW:
														nextRow = true;
														break;
										}
										if (nextRow) {
												break;
										}
								}
						}
				}
				final KeyValue finalKV = filterState.produceAccumulatedKeyValue();
				if (finalKV != null) {
						filteredCells.add(finalKV);
				}
				if (filteredCells.isEmpty()) {
						return null;
				} else {
						return new Result(filteredCells);
				}
		}

		@Override
		public DDLFilter newDDLFilter(String transactionId) throws IOException {
				return new DDLFilter(
								transactionStore.getTransaction(control.transactionIdFromString(transactionId)),
								transactionStore
				);
		}
}
