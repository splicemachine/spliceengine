package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.Txn;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class BaseSIFilterPacked<Data> extends FilterBase implements HasPredicateFilter, Writable {
    private Txn txn;
		private TransactionReadController<KeyValue,Get,Scan> readController;
		private EntryPredicateFilter predicateFilter;
		private TxnFilter<Data> filterState = null;
		private boolean countStar = false;
		private ReadResolver readResolver;

		public BaseSIFilterPacked() {
		}

		public BaseSIFilterPacked(TxnFilter<Data> filterState){
				this.filterState = filterState;
		}

		public BaseSIFilterPacked(Txn txn,
													ReadResolver resolver,
													EntryPredicateFilter predicateFilter,
													TransactionReadController<KeyValue,Get, Scan> readController,
													boolean countStar) throws IOException {
				this.txn = txn;
				this.readResolver = resolver;
				this.predicateFilter = predicateFilter;
				this.readController = readController;
				this.countStar = countStar;
		}

		@Override
		public long getBytesVisited(){
				if(filterState==null) return 0l;
				PackedTxnFilter<KeyValue> packed = (PackedTxnFilter<KeyValue>)filterState;
				@SuppressWarnings("unchecked") RowAccumulator accumulator = packed.getAccumulator();
				return accumulator.getBytesVisited();
		}

		@Override
		public EntryPredicateFilter getFilter(){
				return predicateFilter;
		}

		public ReturnCode internalFilter(Data keyValue) {
				try {
						initFilterStateIfNeeded();
						return filterState.filterKeyValue(keyValue);
				} catch (IOException e) {
						throw new RuntimeException(e);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
		}

		@SuppressWarnings("unchecked")
		private void initFilterStateIfNeeded() throws IOException {
				if (filterState == null) {
						filterState = readController.newFilterStatePacked(readResolver, predicateFilter, txn, countStar);
//						filterState = readController.newFilterStatePacked(tableName, rollForwardQueue, predicateFilter,
//										Long.parseLong(transactionIdString), countStar);
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

		public void internalFilterRow(List<Data> keyValues) {
				try {
						initFilterStateIfNeeded();
				} catch (IOException e) {
						throw new RuntimeException(e);
				}
				if (!filterRow())
						keyValues.remove(0);
				final Data accumulatedValue = filterState.produceAccumulatedKeyValue();
				if (accumulatedValue != null) {
						keyValues.add(accumulatedValue);
				}
		}

		@Override
		public void reset() {
				if (filterState != null)
						filterState.nextRow();
		}

		@Override public void readFields(DataInput in) throws IOException { }

		@Override
		public void write(DataOutput out) throws IOException {
				throw new UnsupportedOperationException("This filter should not be serialized");
		}
}