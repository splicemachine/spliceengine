package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Sets;
import com.splicemachine.derby.utils.StandardSupplier;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class AggregateBuffer {
    private byte[][] keys;
    private BufferedAggregator[] values;
    private final SpliceGenericAggregator[] aggregates;
    //true if we should eliminate duplicates, false if we should not
    private final boolean eliminateDuplicates;
    private final boolean shouldMerge;
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private final WarningCollector warningCollector;

    private int currentSize= 0;
    private GroupedRow groupedRow;

    /*
     * Performance enhancer--means that evict() will always start close to the lowest filled
     * entry in the buffer, instead of having to start at the beginning of the array each time.
     */
    private int firstFilledPosition=Integer.MAX_VALUE;

    public AggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           boolean eliminateDuplicates,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector){
        this(maxSize, aggregators, eliminateDuplicates, emptyRowSupplier, warningCollector,true);
    }
    public AggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           boolean eliminateDuplicates,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector,
                           boolean shouldMerge) {
        this.aggregates = aggregators;
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
        this.shouldMerge = shouldMerge;

        //find smallest power of 2 that contains maxSize
        int bufferSize = 1;
        while(bufferSize<maxSize)
            bufferSize<<=1;

        this.keys = new byte[bufferSize][];
        this.values = new BufferedAggregator[bufferSize];
        this.eliminateDuplicates = eliminateDuplicates;
    }

    public GroupedRow add(byte[] groupingKey, ExecRow nextRow) throws StandardException {
        GroupedRow evicted = evictIfNeeded();

        int byteHash = getHash(groupingKey);

        boolean found;
        int position = byteHash - 1;
        BufferedAggregator aggregate;
        do {
            //linear collision resolution
            position = (position + 1) & (keys.length - 1);
            byte[] key = keys[position];
            aggregate = values[position];
            found = key==null||Arrays.equals(keys[position],groupingKey) || aggregate==null || !aggregate.isInitialized();
        } while (!found);

        if (aggregate == null) {
            //empty slot, create one and initialize it
            aggregate = new BufferedAggregator(aggregates, eliminateDuplicates, shouldMerge,
                    emptyRowSupplier, warningCollector);
            values[position] = aggregate;
        }

        if (!aggregate.isInitialized()){
            keys[position] = groupingKey;
            aggregate.initialize(nextRow);
            currentSize++;
        }else
            aggregate.merge(nextRow);
        if(position<firstFilledPosition)
            firstFilledPosition=position;

        return evicted;
    }

    public GroupedRow getFinalizedRow() throws StandardException{
        return evict();
    }

    public int size(){
        return currentSize;
    }

/*********************************************************************************************************************/
    /*private helper functions*/

    private GroupedRow evictIfNeeded()throws StandardException{
        return currentSize >= keys.length ? evict() : null;
    }

    private GroupedRow evict() throws StandardException {

        //evict the first non-null entry in the buffer
        int evictPos=firstFilledPosition-1;
        byte[] groupedKey;
        boolean found;
        BufferedAggregator aggregate;
        do{
            evictPos++;
            groupedKey = keys[evictPos];
            aggregate = values[evictPos];
            found = groupedKey!=null && aggregate!=null
                    && aggregate.isInitialized();
        }while(!found && evictPos<keys.length);

        if(evictPos>=keys.length)
            return null; //empty buffer

        firstFilledPosition = evictPos+1;
        if(groupedRow==null)
            groupedRow = new GroupedRow();
        aggregate = values[evictPos];
        currentSize--;
        ExecRow row = aggregate.finish();
        groupedRow.setRow(row);
        groupedRow.setGroupingKey(groupedKey);

        return groupedRow;
    }

    private int getHash(byte[] groupingKey) {
        int h = 1;
        for(byte byt:groupingKey){
            h = 31*h + byt;
        }

        //smear the hash around a bit for better distribution
        h ^= (h>>>20)^(h>>>12);
        return h ^(h>>>7)^(h>>>4);
    }

    private static class BufferedAggregator{
        protected final SpliceGenericAggregator[] aggregates;
        private final boolean eliminateDuplicates;
        private final StandardSupplier<ExecRow> emptyRowSupplier;
        private final WarningCollector warningCollector;
        private final boolean shouldMerge;

        private HashSet<DataValueDescriptor> uniqueValues;
        private ExecRow currentRow;

        protected BufferedAggregator(SpliceGenericAggregator[] aggregates,
                                     boolean eliminateDuplicates,
                                     boolean shouldMerge,
                                     StandardSupplier<ExecRow> emptyRowSupplier,
                                     WarningCollector warningCollector) {
            this.aggregates= aggregates;
            this.eliminateDuplicates = eliminateDuplicates;
            this.emptyRowSupplier = emptyRowSupplier;
            this.warningCollector = warningCollector;
            this.shouldMerge = shouldMerge;
        }

        public void initialize(ExecRow row) throws StandardException{
            this.currentRow = row;
            for(SpliceGenericAggregator aggregator:aggregates){
                aggregator.initialize(currentRow);
                filterDistincts(currentRow, aggregator);
                aggregator.accumulate(currentRow,currentRow);
            }
        }

        public void merge(ExecRow newRow) throws StandardException{
            for(SpliceGenericAggregator aggregator:aggregates){
                if (!filterDistincts(newRow, aggregator)){
                    if(shouldMerge)
                        aggregator.merge(newRow,currentRow);
                    else
                        aggregator.accumulate(newRow,currentRow);
                }
            }
        }


        private boolean filterDistincts(ExecRow newRow, SpliceGenericAggregator aggregator) throws StandardException {
            if(aggregator.isDistinct()){
                if(eliminateDuplicates)
                    return true;
                if(uniqueValues==null)
                    uniqueValues = Sets.newHashSet();

                DataValueDescriptor uniqueValue = aggregator.getInputColumnValue(newRow).cloneValue(false);
                if(uniqueValues.contains(uniqueValue))
                    return true;

                uniqueValues.add(uniqueValue);
            }
            return false;
        }

        public boolean isInitialized() {
            return currentRow!=null;
        }

        public ExecRow finish() throws StandardException{
            if(currentRow==null)
                currentRow = emptyRowSupplier.get();

            boolean eliminatedNulls = false;
            for(SpliceGenericAggregator aggregate:aggregates){
                if(aggregate.finish(currentRow))
                    eliminatedNulls=true;
            }
            if(eliminatedNulls)
                warningCollector.addWarning(SQLState.LANG_NULL_ELIMINATED_IN_SET_FUNCTION);

            //once finished, return this to an unitialized state so it can be reused
            ExecRow toReturn = currentRow;
            currentRow= null;
            if(uniqueValues!=null)
                uniqueValues.clear();
            return toReturn;
        }
    }

}
