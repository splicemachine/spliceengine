package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.TransactionReadController;
import com.splicemachine.si.api.Txn;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.HasPredicateFilter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.DataInput;
import java.io.DataOutput;
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
													TransactionReadController<KeyValue,Get, Scan> readController,
													boolean countStar) throws IOException {
			super(txn,resolver,predicateFilter,readController,countStar);
		}

		@Override
		public ReturnCode filterKeyValue(Cell keyValue) {
				return internalFilter(keyValue);
		}
}