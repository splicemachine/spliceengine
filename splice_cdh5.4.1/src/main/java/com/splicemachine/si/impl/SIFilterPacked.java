package com.splicemachine.si.impl;

import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.filter.BaseSIFilterPacked;
import com.splicemachine.storage.EntryPredicateFilter;
import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilterPacked extends FilterBase implements Writeable {
        BaseSIFilterPacked<Cell,Filter,Get,Result,ReturnCode,Scan> baseSIFilterPacked;

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
		public Filter.ReturnCode filterKeyValue(Cell keyValue) {
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
    @Override public void readFields(DataInput in) throws IOException { }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("This filter should not be serialized");
    }


}