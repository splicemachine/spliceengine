package com.splicemachine.derby.impl.sql.execute.operations.sort;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractStandardIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.encoding.MultiFieldEncoder;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;
import java.util.List;

/**
 * Aggregator for use with Sinking aggregates.
 *
 * Unlike {@link ScanDistinctIterator}, this implementation makes a distinction
 * between distinct aggregates and non-distinct aggregates.
 *
 * @author Scott Fines
 * Created on: 11/5/13
 */
public class SinkSortIterator extends AbstractStandardIterator {
		private final DistinctSortAggregateBuffer distinctBuffer;
		private final int[] sortColumns;
		private boolean[] sortColumnOrder;
		private long rowsRead;
		private boolean completed;
		private MultiFieldEncoder encoder;
		private GroupedRow groupedRow;

		public SinkSortIterator(DistinctSortAggregateBuffer distinctBuffer,
														StandardIterator<ExecRow> source,
														int[] sortColumns,
														boolean[] sortColumnOrder) {
				super(source);
				this.distinctBuffer = distinctBuffer;
				this.sortColumns = sortColumns;
				this.sortColumnOrder = sortColumnOrder;
		}

		@Override
		public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(groupedRow==null)
						groupedRow = new GroupedRow();

				if (distinctBuffer == null) {
						groupedRow.setRow(source.next(spliceRuntimeContext));
						return groupedRow;
				}
				if(completed){
						if(distinctBuffer.size()>0){
								return distinctBuffer.getFinalizedRow();
						}
						else return null;
				}

				boolean shouldContinue;
				GroupedRow toReturn = null;
				do{
						SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
						ExecRow nextRow = source.next(spliceRuntimeContext);
						shouldContinue = nextRow!=null;
						if(!shouldContinue)
								continue; //iterator exhausted, break from the loop
						toReturn = distinctBuffer.add(groupingKey(nextRow), nextRow);
						shouldContinue = toReturn==null;
						rowsRead++;
				}while(shouldContinue);

				if(toReturn!=null)
						return toReturn;
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
				completed=true;

				//we've exhausted the iterator, so return an entry from the buffer
				if(distinctBuffer.size()>0)
						return distinctBuffer.getFinalizedRow();

				//the buffer has nothing in it either, just return null
				return null;
		}

		public long getRowsRead(){
				return rowsRead;
		}

		private byte[] groupingKey(ExecRow nextRow) throws StandardException {
				if(encoder==null)
						encoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),sortColumns.length);
				encoder.reset();
				//noinspection RedundantCast
				((KeyMarshall)KeyType.BARE).encodeKey(nextRow.getRowArray(), sortColumns, sortColumnOrder, null, encoder);
				return encoder.build();
		}
}
