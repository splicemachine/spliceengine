package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * TaskSplitter which splits a task based on the StoreFiles in a Region.
 *
 * @author Scott Fines
 * Date: 4/14/14
 */
public class StoreFileTaskSplitter implements TaskSplitter{
		private final HRegion region;
		private final int maxTasksPerSplit;
		private final long minSplitSizeBytes; //the minimum size a store file has to see before splitting

		public StoreFileTaskSplitter(HRegion region,int maxTasksPerSplit,long minSplitSizeBytes) {
				this.region = region;
				this.maxTasksPerSplit = maxTasksPerSplit;
				this.minSplitSizeBytes = minSplitSizeBytes;
		}

		@Override
				public List<byte[]> split(RegionTask task, byte[] taskStart, byte[] taskStop) throws IOException {

				Store store = region.getStore(SpliceConstants.DEFAULT_FAMILY_BYTES);

				List<StoreFile> storeFiles = store.getStorefiles();
				List<byte[]> possibleSplitPoints = Lists.newArrayListWithCapacity(2*storeFiles.size());
				long scanSize = 0l;
				for(StoreFile storeFile:storeFiles){
						StoreFile.Reader reader = storeFile.getReader();
						byte[] startKey = reader.getFirstKey();
						if(Bytes.compareTo(startKey,taskStop)>=0) continue; //this store file isn't involved
						byte[] stopKey = reader.getLastKey();
						if(Bytes.compareTo(taskStart,stopKey)<0) continue; //this store file isn't involved

						scanSize+=reader.length(); //assume we have to read the entire store file
						byte[] midPoint = reader.midkey();
						boolean midAdded =false;
						if(BytesUtil.startComparator.compare(taskStart,startKey)<=0)
								possibleSplitPoints.add(startKey);
						else if(BytesUtil.startComparator.compare(taskStart,midPoint)<=0){
								possibleSplitPoints.add(midPoint);
								midAdded=true;
						}

						if(BytesUtil.endComparator.compare(taskStop,stopKey)>0)
								possibleSplitPoints.add(stopKey);
						else if(!midAdded && BytesUtil.endComparator.compare(taskStop,midPoint)>0)
								possibleSplitPoints.add(midPoint);
				}
				if(scanSize<minSplitSizeBytes||possibleSplitPoints.size()<=0){
						//we are too small to bother parallelizing, just return one.
						return Collections.emptyList();
				}

				Collections.sort(possibleSplitPoints,Bytes.BYTES_COMPARATOR);
				int numSplits = Math.min(maxTasksPerSplit,possibleSplitPoints.size());

				return findSplits(0,possibleSplitPoints.size(),numSplits,possibleSplitPoints);
		}

		private List<byte[]> findSplits(int minPos,int maxPos,int maxSplits,List<byte[]> possiblePoints){
				if(maxPos<=minPos||maxSplits<=1) return Lists.newArrayListWithExpectedSize(0);

				int midPointPos = (maxPos-minPos)/2+minPos;
				byte[] midPoint = possiblePoints.get(midPointPos);

				List<byte[]> splits = findSplits(minPos, midPointPos, maxSplits-1,possiblePoints);
				splits.add(midPoint);
				splits.addAll(findSplits(midPointPos, maxPos, maxSplits-splits.size(),possiblePoints));
				return splits;
		}
}
