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
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class SITransactionReadController<OperationWithAttributes,
				Get extends OperationWithAttributes,
				Scan extends OperationWithAttributes,
				Delete extends OperationWithAttributes,
				Put extends OperationWithAttributes,
				OperationStatus,
				Lock,
				Data,Hashable extends Comparable,Result,KeyValue>
				implements TransactionReadController<Get,Scan,Data,Hashable,Result,KeyValue>{
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
		public IFilterState newFilterState(RollForwardQueue<Data, Hashable> rollForwardQueue, TransactionId transactionId, boolean includeSIColumn) throws IOException {
				return new FilterState(dataLib, dataStore, transactionStore, rollForwardQueue, includeSIColumn,
								transactionStore.getImmutableTransaction(transactionId));
		}

		@Override
		@SuppressWarnings("unchecked")
		public IFilterState newFilterStatePacked(String tableName, RollForwardQueue<Data, Hashable> rollForwardQueue, EntryPredicateFilter predicateFilter, TransactionId transactionId, boolean includeSIColumn) throws IOException {
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
		public Result filterResult(IFilterState<KeyValue> filterState, Result result) throws IOException {
				//TODO -sf- this is only used in testing--ignore when production tuning
				final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib = dataStore.dataLib;
				final List<KeyValue> filteredCells = Lists.newArrayList();
				final List<KeyValue> keyValues = dataLib.listResult(result);
				if (keyValues != null) {
						Data qualifierToSkip = null;
						Data familyToSkip = null;
						Data currentRowKey = null;
						for (KeyValue keyValue : keyValues) {
								final Data rowKey = dataLib.getKeyValueRow(keyValue);
								if (currentRowKey == null || !dataLib.valuesEqual(currentRowKey, rowKey)) {
										currentRowKey = rowKey;
										filterNextRow(filterState);
								}
								//noinspection StatementWithEmptyBody
								if (familyToSkip != null
												&& dataLib.valuesEqual(familyToSkip, dataLib.getKeyValueFamily(keyValue))
												&& dataLib.valuesEqual(qualifierToSkip, dataLib.getKeyValueQualifier(keyValue))) {
										// skipping to next column
								} else {
										familyToSkip = null;
										qualifierToSkip = null;
										boolean nextRow = false;
										Filter.ReturnCode returnCode = filterKeyValue(filterState, keyValue);
										switch (returnCode) {
												case SKIP:
														break;
												case INCLUDE:
														filteredCells.add(keyValue);
														break;
												case NEXT_COL:
														qualifierToSkip = dataLib.getKeyValueQualifier(keyValue);
														familyToSkip = dataLib.getKeyValueFamily(keyValue);
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
				final KeyValue finalKeyValue = filterState.produceAccumulatedKeyValue();
				if (finalKeyValue != null) {
						filteredCells.add(finalKeyValue);
				}
				if (filteredCells.isEmpty()) {
						return null;
				} else {
						return dataLib.newResult(dataLib.getResultKey(result), filteredCells);
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
