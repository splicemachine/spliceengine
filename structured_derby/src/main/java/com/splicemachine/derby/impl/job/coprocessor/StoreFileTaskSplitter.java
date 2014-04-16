package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.collect.Sets;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.utils.SingletonSortedSet;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;

/**
 * TaskSplitter which splits a task based on the StoreFiles in a Region.
 *
 * @author Scott Fines
 * Date: 4/14/14
 */
public class StoreFileTaskSplitter implements TaskSplitter{
		private static final Logger LOG = Logger.getLogger(StoreFileTaskSplitter.class);
		private final HRegion region;
		private final int maxTasksPerSplit;
		private final long minSplitSizeBytes; //the minimum size a store file has to see before splitting

		public StoreFileTaskSplitter(HRegion region,int maxTasksPerSplit,long minSplitSizeBytes) {
				this.region = region;
				this.maxTasksPerSplit = maxTasksPerSplit;
				this.minSplitSizeBytes = minSplitSizeBytes;
		}

		@Override
		public SortedSet<SizedInterval> split(final RegionTask task, final byte[] taskStart, byte[] taskStop) throws IOException {
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"Attempting to split task %s with bounds [%s,%s)",task,Bytes.toStringBinary(taskStart),Bytes.toStringBinary(taskStop));

				boolean isQualified = !matchesRegionBounds(region,taskStart,taskStop);
				Store store = region.getStore(SpliceConstants.DEFAULT_FAMILY_BYTES);
				HRegionUtil.lockStore(store);
				try{
						List<StoreFile> storeFiles = store.getStorefiles();
						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG,"Region %s has %d store files",region,storeFiles.size());
						NavigableSet<SizedInterval> possibleScanIntervals = Sets.newTreeSet();
						/*
						 * StoreFiles can be split into two intervals: [start,midpoint),[midpoint,end). Thus,
						 * we have the following situations:
						 *
						 * 1. taskStop <= start 					=> store file not involved, ignore it
						 * 2. taskStart >= end 						=> store file not involved, ignore it
						 * 3. midpoint < taskStart <end 	=> store file from [taskStart,end) involved
						 * 4. start < taskStop <=midPoint	=> store file from [start,taskStop) involved
						 * 5. start <=taskStart < midPoint < taskStop <=end => both intervals in store file involved.
						 *
						 * We want to create a sequence of tasks which covers all the intervals involved.
						 *
						 * To do this, we first split the store file into it's two component intervals, then
						 * throw away intervals not involved. What remains are possible split points. We
						 * then merge together those splits until we have either
						 *
						 * A. all splits of smaller than the threshold
						 * B. the maximum number of splits allowed.
						 *
						 */
						for(StoreFile storeFile:storeFiles){
								StoreFile.Reader reader = storeFile.getReader();
								byte[] startKey = reader.getFirstKey();
								// eliminate case 1 store files--taskStop <= start of store file
								if(isQualified && BytesUtil.endComparator.compare(startKey,taskStop)>=0) continue;
								// eliminate case 2 store files --taskStart >=end of the store file
								byte[] endKey = reader.getLastKey();
								if(isQualified && BytesUtil.startComparator.compare(endKey,taskStart)<=0) continue;

								//either [start,midKey), [midKey,end), or both are involved in the process
								byte[] midKey = reader.midkey();
								long size = reader.length();
								if(size/2<minSplitSizeBytes){
										/*
								 		 * The entire file is smaller than the splitting threshold, so treat it as one possible split
								 		 */
										possibleScanIntervals.add(new SizedInterval(startKey,endKey,size));

								}else{
								/*
								 * This file is larger than the splitting threshold, so split it into two separate intervals
								 */
										if(BytesUtil.startComparator.compare(startKey,midKey)<0){
												//the interval [start,midKey) is involved
												addOrExpand(possibleScanIntervals,new SizedInterval(startKey,midKey,size/2));
										}
										if(BytesUtil.endComparator.compare(midKey,taskStop)<0){
												//the interval [midKey,end) is involved
												addOrExpand(possibleScanIntervals,new SizedInterval(midKey,endKey,size/2));
										}
								}
						}
						//if we don't have any store files, just return our original bounds
						if(possibleScanIntervals.size()<=1){
						/*
						 * We either don't have any store files, or have just one store file.
						 *
						 * When we have only one store file, there might be data in the memstore that we
						 * don't want to avoid scanning. In that case, we just return the original bound (since we
						 * weren't going to split anyway).
						 */
								return new SingletonSortedSet<SizedInterval>(new SizedInterval(taskStart,taskStop,0));
						}
				/*
				 * We now have a list of possible splits, but that amount may exceed the maximum number of
				 * splits that we allow. If that is the case, we need to merge adjacent scans together until
				 * we fall below the threshold.
				 */
						//make sure to set the bounds correctly
						possibleScanIntervals.first().startKey = taskStart;
						possibleScanIntervals.last().endKey = taskStop;
						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG,"There are %d possible splits for bounds [%s,%s):%s",
												possibleScanIntervals.size(),
												Bytes.toStringBinary(taskStart),Bytes.toStringBinary(taskStop),possibleScanIntervals);

						merge(possibleScanIntervals);
						while(possibleScanIntervals.size()>maxTasksPerSplit){
								mergeSmallest(possibleScanIntervals);
						}
						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG,"There are %d splits for bounds [%s,%s) after merging:%s",
												possibleScanIntervals.size(),
												Bytes.toStringBinary(taskStart),Bytes.toStringBinary(taskStop),possibleScanIntervals);
						return possibleScanIntervals;
				}finally{
						HRegionUtil.unlockStore(store);
				}
		}

		private void merge(NavigableSet<SizedInterval> possibleScanIntervals) {
				Iterator<SizedInterval> iter = possibleScanIntervals.iterator();
				SizedInterval first = iter.next();
				while(iter.hasNext()){
						SizedInterval value = iter.next();
						/*
						 * We merge the two together if one of the two following conditions are met
						 * 1. first.startKey <= value.startKey < first.endKey
						 * 2. first.bytes<minSplitSizeBytes
						 */
						if(BytesUtil.startComparator.compare(first.startKey,value.startKey)<=0){
								if(BytesUtil.endComparator.compare(value.startKey,first.endKey)<0){
										if(Bytes.compareTo(first.endKey,value.endKey)<0){
												//expand the end key to include the value's end key
												first.endKey = value.endKey;
										}
										first.bytes+=value.bytes;
										iter.remove();
								}
						} else if(first.bytes<minSplitSizeBytes){
								//we need to merge into the next component
								if(Bytes.compareTo(first.endKey,value.endKey)<0){
										first.endKey = value.endKey;
								}
								first.bytes+=value.bytes;
								iter.remove();
						}else
								first = value;
				}
		}

		private boolean checkCeiling(NavigableSet<SizedInterval> intervals,SizedInterval sizedInterval){
				SizedInterval next = intervals.ceiling(sizedInterval);
				if(next==null) return false;
				if(BytesUtil.startComparator.compare(next.startKey,sizedInterval.startKey)!=0) return false;

				/*
				 * We have the same start key as our ceiling element, so we should
				 * form a range that covers both
				 */
				byte[] oldEnd = next.endKey;
				if(BytesUtil.endComparator.compare(oldEnd,sizedInterval.endKey)<0){
						next.endKey = sizedInterval.endKey;
				}
				next.bytes+=sizedInterval.bytes;
				return true;
		}

		private boolean checkFloor(NavigableSet<SizedInterval> intervals, SizedInterval interval){
				SizedInterval floor = intervals.lower(interval);
				if(floor==null) return false;
				if(BytesUtil.endComparator.compare(floor.endKey,interval.endKey)!=0) return false;

				/*
				 * We have the same end key as our floor element, even though we have a higher start key.
				 * Thus, we are strictly contained in that store file. Add to its bytes and move on
				 */
				floor.bytes+=interval.bytes;
				return true;
		}
		private void addOrExpand(NavigableSet<SizedInterval> intervals,SizedInterval sizedInterval) {
				if(!checkCeiling(intervals,sizedInterval)&&!checkFloor(intervals,sizedInterval))
						intervals.add(sizedInterval);
		}

		private boolean matchesRegionBounds(HRegion region, byte[] taskStart, byte[] taskStop) {
				byte[] regionStart = region.getStartKey();
				return Bytes.equals(taskStart, regionStart) && Bytes.equals(taskStop, region.getEndKey());
		}

		private void mergeSmallest(NavigableSet<SizedInterval> possibleScanIntervals) {
				//find the 2 smallest intervals, and merge them together
				long minSize = Integer.MAX_VALUE;
				Iterator<SizedInterval> iter = possibleScanIntervals.iterator();
				SizedInterval b = iter.next();
				SizedInterval n;
				SizedInterval minElement = b;
				while(iter.hasNext()){
						n = iter.next();

						long size = n.bytes+b.bytes;
						if(size<minSize){
								minSize = size;
								minElement = b;
						}

						b = n;
				}

				SizedInterval smallest = minElement;
				SizedInterval nextSmallest = possibleScanIntervals.higher(smallest);
				possibleScanIntervals.remove(nextSmallest);
				smallest.endKey = nextSmallest.endKey;
		}

		public static void main(String...args) throws Exception{
				byte[] regionStart = Bytes.toBytesBinary("\\xE1\\xC4.\\x00\\xF8Ft\\x19u\\xB4\\x95_\\xC0");
				byte[] regionStop = Bytes.toBytesBinary("\\xE1\\xD0\\xE2\\x00\\xF8F\\xE4\\x1E\\xCF\\xDC73@");
				int compare = BytesUtil.startComparator.compare(regionStart, regionStop);
				System.out.println(compare);

		}
}
