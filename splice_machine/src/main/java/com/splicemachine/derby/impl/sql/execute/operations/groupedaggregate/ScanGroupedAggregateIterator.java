package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class ScanGroupedAggregateIterator extends GroupedAggregateIterator{
    private final GroupedAggregateBuffer buffer;
		private final KeyEncoder groupKeyEncoder;

    public ScanGroupedAggregateIterator(GroupedAggregateBuffer buffer,
                                        StandardIterator<ExecRow> source,
                                        KeyEncoder encoder,
                                        int[] groupColumns,
                                        boolean isRollup) {
        super(source, isRollup, groupColumns);
        this.buffer = buffer;
        this.groupKeyEncoder = encoder;
        int maxEvicted = isRollup ? groupColumns.length + 1 : 1;
        evictedRows = Lists.newArrayListWithCapacity(maxEvicted);
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (groupKeyEncoder != null)
            groupKeyEncoder.close();
    }


    @Override
    public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        //return any previously evicted rows first
        if (evictedRows.size() > 0) {
            return evictedRows.remove(0);
        }
        if (completed) {
            if (buffer.size() > 0) {
                return buffer.getFinalizedRow();
            }
            else return null;
        }

        boolean shouldContinue;
        GroupedRow toReturn = null;
        do {
            SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);
            ExecRow nextRow = source.next(spliceRuntimeContext);
            shouldContinue = nextRow != null;
            if (!shouldContinue)
                break; //iterator exhausted, break from the loop

            toReturn = buffer(nextRow);
            shouldContinue = toReturn == null;
            rowsRead++;
        } while (shouldContinue);

        if (toReturn != null) {
            return toReturn;
        }
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
        completed = true;

        //we've exhausted the iterator, so return an entry from the buffer
        if (buffer.size() > 0) {
            return buffer.getFinalizedRow();
        }

        //the buffer has nothing in it either, just return null
        return null;
    }

		@Override public long getRowsRead() { return rowsRead; }
		@Override public long getRowsMerged() { return buffer.getRowsMerged(); }
		@Override public double getMaxFillRatio() { return buffer.getMaxFillRatio(); }

		protected GroupedRow buffer(ExecRow nextRow) throws StandardException {
        if(!isRollup){
            return buffer.add(getGroupingKey(nextRow),nextRow);
        }else{
            GroupedRow firstEvicted = null;
            rollupRows(nextRow);
            for(ExecRow rollup:rollupRows){
                //we don't need to clone, cause rolling up rows does it for us
                GroupedRow groupedRow = buffer.add(getGroupingKey(rollup),rollup);
                if(groupedRow!=null){
                    if(firstEvicted==null)
                        firstEvicted= groupedRow;
                    else
                        evictedRows.add(groupedRow);
                }
            }
            return firstEvicted;
        }
    }

    private byte[] getGroupingKey(ExecRow rollup) throws StandardException {
				try {
						return groupKeyEncoder.getKey(rollup);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
    }

    private void rollupRows(ExecRow row) throws StandardException{
        if(rollupRows==null){
            rollupRows = isRollup? new ExecRow[groupColumns.length+1]: new ExecRow[1];
        }
        if(!isRollup){
            rollupRows[0] = row;
            return;
        }
        int rollUpPos = groupColumns.length;
        int pos=0;
        ExecRow nextRow = row.getClone();
        do{
            rollupRows[pos] = nextRow;
            if(rollUpPos>0){
                nextRow = nextRow.getClone();
                DataValueDescriptor rollUpCol = nextRow.getColumn(groupColumns[rollUpPos-1]+1);
                rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        }while(rollUpPos>=0);
    }
}
