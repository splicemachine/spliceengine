package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.stats.Counter;
import com.splicemachine.stats.Gauge;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.utils.hash.ByteHash32;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.util.Arrays;

/**
 *
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
 *
 * @author Scott Fines
 * Created on: 11/1/13
 */
public abstract class AbstractAggregateBuffer extends AbstractAggregateBufferConstants {
		protected byte[][] keys;
		protected BufferedAggregator[] values;
		protected final ByteHash32[] hashes;
		protected final SpliceGenericAggregator[] aggregates;
		protected int currentSize= 0;
		protected GroupedRow groupedRow;
		protected int lastEvictedPosition = -1;
		protected int bufferSize = 1;

		/*stats measurement*/
		protected Counter mergeCounter; //counts the number of merges which are incurred
		protected Gauge maxFillRatio;

		public AbstractAggregateBuffer(int maxSize,SpliceGenericAggregator[] aggregators,MetricFactory metricFactory){
				this(maxSize, aggregators, DEFAULT_HASHES,metricFactory);
		}
		public AbstractAggregateBuffer(int maxSize,
																	 SpliceGenericAggregator[] aggregators,
																	 ByteHash32[] hashes,
																	 MetricFactory metricFactory) {
				this.aggregates = aggregators;
				this.hashes = hashes;
				while(bufferSize<maxSize)
						bufferSize<<=1;
				this.keys = new byte[bufferSize][];
				this.mergeCounter = metricFactory.newCounter();
				this.maxFillRatio = metricFactory.newMaxGauge();
				intializeAggregator();
		}

		public GroupedRow add(byte[] groupingKey, ExecRow nextRow) throws StandardException {
				GroupedRow evicted = null;
				boolean found = false;
				byte[] key = null;
				BufferedAggregator aggregate = null;
				int position = 0;
				for(int hashPos=0;hashPos<hashes.length && !found;hashPos++){
						ByteHash32 hashFunction = hashes[hashPos];
						int hashCode = hashFunction.hash(groupingKey,0,groupingKey.length);
						position = hashCode & (keys.length-1);
						for(int i=0;i<5 && !found; i++){
								position = (position+i) & (keys.length-1);
								key = keys[position];
								aggregate = values[position];
								found = key ==null || Arrays.equals(keys[position],groupingKey) || aggregate==null || !aggregate.isInitialized();
						}
				}
//				BufferedAggregator aggregate;
//				int hashCount=0;
//				int byteHash = hashes[0].hash(groupingKey,0,groupingKey.length);
//				byte[] key;
//				do{
//						if(hashCount>0)
//								byteHash+= hashCount*hashes[hashCount].hash(groupingKey,0,groupingKey.length);
//						position = byteHash & (keys.length-1);
//						key = keys[position];
//						aggregate = values[position];
//						found = key==null||Arrays.equals(keys[position],groupingKey) || aggregate==null || !aggregate.isInitialized();
//						hashCount++;
//				} while(!found && hashCount<hashes.length);

				if(!found){
						//evict the last entry
						evicted = getEvictedRow(key,aggregate);
				}

				if (aggregate == null) {
						//empty slot, create one and initialize it
						aggregate = createBufferedAggregator(); // Utilizes AbstractAggregator
						values[position] = aggregate;
				}

				if (!aggregate.isInitialized()){
						keys[position] = groupingKey;
						aggregate.initialize(nextRow);
						currentSize++;
						if(maxFillRatio.isActive())
								maxFillRatio.update((double)currentSize/bufferSize);
				} else {
						mergeCounter.add(1);
						aggregate.merge(nextRow);
				}
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

		/*methods exposing statistics*/
		public long getRowsMerged(){ return mergeCounter.getTotal(); }
		public double getMaxFillRatio(){ return maxFillRatio.getValue(); }

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
						found = groupedKey!=null && aggregate!=null && aggregate.isInitialized();
				} while(!found && visitedCount<values.length);
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

		public abstract BufferedAggregator createBufferedAggregator();
		public abstract void intializeAggregator();
}
