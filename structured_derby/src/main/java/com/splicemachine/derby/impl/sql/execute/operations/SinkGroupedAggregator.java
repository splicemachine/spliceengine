package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Aggregator for use with Sinking aggregates.
 *
 * Unlike {@link ScanGroupedAggregator}, this implementation makes a distinction
 * between distinct aggregates and non-distinct aggregates.
 *
 * @author Scott Fines
 * Created on: 11/5/13
 */
public class SinkGroupedAggregator implements StandardIterator<GroupedRow> {
    private final AggregateBuffer nonDistinctBuffer;
    private final AggregateBuffer distinctBuffer;
    private final StandardIterator<ExecRow> source;
    private final boolean isRollup;

    /*
     * This array is of the form
     * <grouping columns> <non-grouped unique columns>
     */
    private final int[] allKeyColumns;
    private final boolean[] allSortOrders;

    private final int[] groupColumns;
    private final boolean[] groupSortOrder;

    private boolean completed = false;
    private List<GroupedRow> evictedRows;
    private ExecRow[] rollupRows;

    //cached fields for performance
    private MultiFieldEncoder groupKeyEncoder;
    private MultiFieldEncoder allKeyEncoder;
    private KeyMarshall keyHasher = KeyType.BARE;


    public SinkGroupedAggregator(AggregateBuffer nonDistinctBuffer,
                                 AggregateBuffer distinctBuffer,
                                 StandardIterator<ExecRow> source,
                                 boolean rollup,
                                 int[] groupColumns,
                                 boolean[] groupSortOrder,
                                 int[] nonGroupedUniqueColumns) {
        this.nonDistinctBuffer = nonDistinctBuffer;
        this.distinctBuffer = distinctBuffer;
        this.source = source;
        isRollup = rollup;
        this.groupColumns = groupColumns;
        this.groupSortOrder = groupSortOrder;

        allKeyColumns = new int[groupColumns.length+nonGroupedUniqueColumns.length];
        System.arraycopy(groupColumns,0,allKeyColumns,0,groupColumns.length);
        System.arraycopy(nonGroupedUniqueColumns,0,allKeyColumns,groupColumns.length,nonGroupedUniqueColumns.length);

        allSortOrders = new boolean[groupColumns.length+nonGroupedUniqueColumns.length];
        System.arraycopy(groupSortOrder,0,allSortOrders,0,groupSortOrder.length);
        Arrays.fill(allSortOrders,groupSortOrder.length,allSortOrders.length,true);

        int maxEvictedSize = isRollup? groupColumns.length: 1;
        evictedRows = Lists.newArrayListWithCapacity(maxEvictedSize);
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }

    @Override
    public GroupedRow next() throws StandardException, IOException {
        if(evictedRows.size()>0)
            return evictedRows.remove(0);
        if(completed){
            if(nonDistinctBuffer.size()>0){
                GroupedRow finalizedRow = nonDistinctBuffer.getFinalizedRow();
                finalizedRow.setDistinct(false);
                return finalizedRow;
            } else if(distinctBuffer.size()>0){
                GroupedRow finalizedRow = distinctBuffer.getFinalizedRow();
                finalizedRow.setDistinct(true);
                return finalizedRow;
            }
            else return null;
        }

        boolean shouldContinue;
        GroupedRow toReturn = null;
        do{
            ExecRow nextRow = source.next();
            shouldContinue = nextRow!=null;
            if(!shouldContinue)
                continue; //iterator exhausted, break from the loop

            toReturn = buffer(nextRow);
            shouldContinue = toReturn==null;
        }while(shouldContinue);

        if(toReturn!=null)
            return toReturn;
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
        completed=true;

        //we've exhausted the iterator, so return an entry from the buffer
        if(nonDistinctBuffer.size()>0)
            return nonDistinctBuffer.getFinalizedRow();
        else if(distinctBuffer.size()>0)
            return distinctBuffer.getFinalizedRow();

        //the buffer has nothing in it either, just return null
        return null;
    }

    private GroupedRow buffer(ExecRow nextRow) throws StandardException {
        GroupedRow firstEvicted = null;
        if(!isRollup){
            return bufferRow(nextRow.getClone());
        }else{
            rollupRows(nextRow);
            for(ExecRow rollup:rollupRows){
                //we don't need to clone, cause rolling up rows does it for us
                GroupedRow groupedRow = bufferRow(rollup);

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

    @Override
    public void close() throws StandardException, IOException {
        source.close();
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

    private GroupedRow bufferRow(ExecRow nextRow) throws StandardException {
        GroupedRow firstEvicted;
        ExecRow clone = nextRow.getClone();
        firstEvicted = nonDistinctBuffer.add(nonDistinctGroupingKey(nextRow), nextRow);
        GroupedRow distinct = distinctBuffer.add(distinctGroupingKey(clone),clone);
        if(firstEvicted!=null){
            firstEvicted.setDistinct(false);
            if(distinct!=null){
                distinct.setDistinct(true);
                evictedRows.add(distinct);
            }
        } else if(distinct!=null){
            distinct.setDistinct(true);
            firstEvicted = distinct;
        }
        return firstEvicted;
    }

    private byte[] distinctGroupingKey(ExecRow nextRow) throws StandardException{
        if(allKeyEncoder==null)
            allKeyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),allKeyColumns.length);

        allKeyEncoder.reset();
        keyHasher.encodeKey(nextRow.getRowArray(),allKeyColumns,allSortOrders,null,allKeyEncoder);
        return allKeyEncoder.build();
    }

    private byte[] nonDistinctGroupingKey(ExecRow nextRow) throws StandardException {
        if(groupKeyEncoder==null)
            groupKeyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),groupColumns.length);

        groupKeyEncoder.reset();
        keyHasher.encodeKey(nextRow.getRowArray(),groupColumns,groupSortOrder,null,groupKeyEncoder);
        return groupKeyEncoder.build();
    }
}
