package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.collect.Sets;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.utils.hash.ByteHash32;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class AggregateBuffer {
    private static final Logger LOG = Logger.getLogger(AggregateBuffer.class);
		private static final ByteHash32[] DEFAULT_HASHES = new ByteHash32[]{
						HashFunctions.murmur3(0),
						HashFunctions.murmur3(5),
						HashFunctions.murmur3(7)
		};
		private byte[][] keys;
    private BufferedAggregator[] values;
		private final ByteHash32[] hashes;
    private final SpliceGenericAggregator[] aggregates;
    //true if we should eliminate duplicates, false if we should not
    private final boolean eliminateDuplicates;
    private final boolean shouldMerge;
    private final StandardSupplier<ExecRow> emptyRowSupplier;
    private final WarningCollector warningCollector;

    private int currentSize= 0;
    private GroupedRow groupedRow;

    private int lastEvictedPosition = -1;

    public AggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           boolean eliminateDuplicates,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector){
        this(maxSize, aggregators, eliminateDuplicates, emptyRowSupplier, warningCollector,false);
    }

		public AggregateBuffer(int maxSize,
													 SpliceGenericAggregator[] aggregators,
													 boolean eliminateDuplicates,
													 StandardSupplier<ExecRow> emptyRowSupplier,
													 WarningCollector warningCollector,
													 boolean shouldMerge) {
				this(maxSize,aggregators,eliminateDuplicates,emptyRowSupplier,warningCollector,shouldMerge,DEFAULT_HASHES);
		}

    public AggregateBuffer(int maxSize,
                           SpliceGenericAggregator[] aggregators,
                           boolean eliminateDuplicates,
                           StandardSupplier<ExecRow> emptyRowSupplier,
                           WarningCollector warningCollector,
                           boolean shouldMerge,
													 ByteHash32[] hashes) {
        this.aggregates = aggregators;
        this.emptyRowSupplier = emptyRowSupplier;
        this.warningCollector = warningCollector;
        this.shouldMerge = shouldMerge;
				this.hashes = hashes;

        //find smallest power of 2 that contains maxSize
        int bufferSize = 1;
        while(bufferSize<maxSize)
            bufferSize<<=1;

        this.keys = new byte[bufferSize][];
        this.values = new BufferedAggregator[bufferSize];
        this.eliminateDuplicates = eliminateDuplicates;
    }


    public GroupedRow add(byte[] groupingKey, ExecRow nextRow) throws StandardException {
        GroupedRow evicted = null;

				/*
				 * We use hashing to give us an expected O(1) insertion.
				 *
				 * There are lots of hashing strategies: Linear Probing,
				 * Quadratic Probing, and Double hashing are the three most common
				 * approaches. However, the naive approach isn't great
				 *
				 * Consider the case when the buffer is full. In that situation,
				 * to find the next position, we have to check EVERY entry in the
				 * buffer before knowing that we can evict an entry. For large buffers
				 * this is prohibitive.
				 *
				 * But when you think of it, the main reason these were structured this
				 * way was to support efficient lookups, of which this buffer does not do.
				 *
				 * So here we take a bounded-eviction approach to enforce an O(1) insertion
				 * performance, at the cost of potentially evicting before the buffer is entirely
				 * full.
				 *
				 * What we do is probe a fixed (configurable) number of times. If a collision
				 * is detected, then the next probe is used to move along, just as in other
				 * collision resolutions. However, if the fixed number of probes has been
				 * exhausted and only collisions are seen, then the last seen entry that
				 * is ALREADY present is evicted, and the new insertion is put in its place.
				 *
				 * This ensures a fixed-time insertion and a fixed-time eviction, at the cost
				 * of potentially evicting an entry before the buffer is full, and also
				 * making linear evictions more expensive since they must probe through (potentially)
				 * many empty entries to find the next evictable position.
				 */
				boolean found = false;
				int position =0;
				BufferedAggregator aggregate;
				int visitedCount=-1;
				int hashCount=0;
				int byteHash = hashes[0].hash(groupingKey,0,groupingKey.length);
				byte[] key;
				do{
						visitedCount++;
						if(hashCount>0)
								byteHash+= hashCount*hashes[hashCount].hash(groupingKey,0,groupingKey.length);
						position = byteHash & (keys.length-1);
						key = keys[position];
						aggregate = values[position];
						found = key==null||Arrays.equals(keys[position],groupingKey) || aggregate==null || !aggregate.isInitialized();
						hashCount++;
				} while(!found && hashCount<hashes.length);

        if(!found){
						//evict the last entry
						evicted = getEvictedRow(key,aggregate);
        }


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

        return evicted;
    }

		public GroupedRow getFinalizedRow() throws StandardException{
        return evict();
    }

    public int size(){
        return currentSize;
    }

    public boolean hasAggregates(){
        return aggregates!=null && aggregates.length>0;
    }

/*********************************************************************************************************************/
    /*private helper functions*/

    private GroupedRow evict() throws StandardException {

        //evict the first non-null entry in the buffer
        int evictPos=lastEvictedPosition;
        byte[] groupedKey;
        boolean found;
        BufferedAggregator aggregate;
        int visitedCount=-1;
        do{
            evictPos = (evictPos + 1) & (keys.length - 1); //fun optimization because we know the size is a power of 2
            visitedCount++;
            groupedKey = keys[evictPos];
            aggregate = values[evictPos];
            found = groupedKey!=null && aggregate!=null
                    && aggregate.isInitialized();
        }while(!found && visitedCount<values.length);

        if(evictPos>=keys.length)
            return null; //empty buffer

        lastEvictedPosition = evictPos;

				return getEvictedRow(groupedKey,values[evictPos]);
    }

		private GroupedRow getEvictedRow(byte[] groupedKey,BufferedAggregator aggregate) throws StandardException {
				if(groupedRow==null)
						groupedRow = new GroupedRow();
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

        private IntObjectMap<HashSet<DataValueDescriptor>> uniqueValues;
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
                boolean shouldAdd = !shouldMerge ||aggregator.isInitialized(currentRow);
                aggregator.initialize(currentRow);
                filterDistincts(currentRow, aggregator,shouldAdd);
                //if shouldMerge is true, then we don't want to accumulate, it'll mess up the accumulations
                if(!shouldMerge)
                    aggregator.accumulate(currentRow,currentRow);
            }
        }

        public void merge(ExecRow newRow) throws StandardException{
            for(SpliceGenericAggregator aggregator:aggregates){
                boolean shouldAdd = aggregator.isInitialized(newRow);
                if (!filterDistincts(newRow, aggregator, shouldAdd)){
                    if(!shouldAdd)
                        aggregator.initialize(newRow);

                    if(shouldMerge)
                        aggregator.merge(newRow,currentRow);
                    else
                        aggregator.accumulate(newRow,currentRow);
                }
            }
        }


        private boolean filterDistincts(ExecRow newRow,
                                        SpliceGenericAggregator aggregator,
                                        boolean addEntry) throws StandardException {
            if(aggregator.isDistinct()){
                if(uniqueValues==null)
                    uniqueValues = IntObjectOpenHashMap.newInstance();

                int inputColNum = aggregator.getAggregatorInfo().getInputColNum();
                HashSet<DataValueDescriptor> uniqueVals = uniqueValues.get(inputColNum);
                if(uniqueVals==null){
                    uniqueVals = Sets.newHashSet();
                    uniqueValues.put(inputColNum,uniqueVals);
                }

                DataValueDescriptor uniqueValue = aggregator.getInputColumnValue(newRow).cloneValue(false);
                if(uniqueVals.contains(uniqueValue)){
                    if(LOG.isTraceEnabled())
                        LOG.trace("Aggregator "+ aggregator+" is removing entry "+ newRow);
                    return true;
                }

                if(addEntry)
                    uniqueVals.add(uniqueValue);
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
