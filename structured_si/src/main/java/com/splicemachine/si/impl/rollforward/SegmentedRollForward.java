package com.splicemachine.si.impl.rollforward;

import com.google.common.collect.Lists;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RollForward strategy which relies on keeping track of writes instead of queueing.
 *
 * The essential idea is as follows:
 *
 * First, we split the region into N disjoint <em>segments</em>. Each segment manages a range
 * of row keys. When a row key is submitted for roll forward, the segment which owns that key
 * increments a counter <em>and</em> updates its estimate for the number of distinct transactions
 * which have written to the location. Periodically, this segment checks for two criteria:
 *
 * 1. If the number of unresolved rows exceeds a threshold
 * 2. The number of distinct transactions exceeds a threshold
 *
 * If both of those are true, then it submits an <em>Action</em>. This action will attempt to
 * perform the actual roll forward for <em>all rows</em> contained in the segment which has exceeded
 * its thresholds.
 *
 * As each row is resolved, it decrements the unresolved counter for the segment. This helps to
 * keep the counter up-to-date and prevents excessive attempts to resolve the same segment over and over again.
 *
 * Until a Statistics engine is fully implemented, this is necessarily a poor implementation--we have no
 * information about the distribution of row keys (even approximately), so some segments may receive
 * significantly more traffic than others, which will result in those segments being rolled forward
 * more often than others. In the future, we will want to re-implement this using a more sophisticated
 * statistical analysis that keeps the segments sized to equalize traffic (as much as possible)--e.g. an
 * equi-depth rather than an equi-width histogram.
 *
 * @author Scott Fines
 * Date: 7/2/14
 */
public class SegmentedRollForward implements RollForward {
    private static final Logger LOG = Logger.getLogger(SegmentedRollForward.class);

    public static class Context{
				private final RegionSegment segment;
        private final RollForwardStatus status;

				public Context(RegionSegment segment,RollForwardStatus status) {
						this.segment = segment;
            this.status = status;
				}

				public void rowResolved(){
            segment.rowResolved();
        }
				public void complete(){ segment.markCompleted(); }
		}

		public interface Action{
				void submitAction(HRegion region,byte[] startKey, byte[] stopKey,Context context);
		}

    public static final Action NOOP_ACTION = new Action() {
        @Override public void submitAction(HRegion region, byte[] startKey, byte[] stopKey, Context context) {  }
    };

		private final HRegion region;
		/*
		 * We use our own internal RegionSegment implementations here, which
		 * kind of sucks--once we have a Statistics Engine, we will likely want to build
		 * a more generalized RegionSegment mechanism which contains better information
		 */
		private final RegionSegment[] segments;

		private final long segmentCheckInterval = (1<<10)-1; //check every 1K updates
		private final long rollForwardRowThreshold;
		private final long rollForwardTransactionThreshold;

		private final Action action;

		private final AtomicLong updateCount = new AtomicLong(0l);
    private final Hash32 hashFunction = HashFunctions.murmur3(0);
    private final RollForwardStatus status;

		public SegmentedRollForward(final HRegion region,
																final ScheduledExecutorService rollForwardScheduler,
																int numSegments,
																long rollForwardRowThreshold,
																long rollForwardTransactionThreshold,
																final @ThreadSafe Action action,
                                final RollForwardStatus status) {
				this.region = region;
				this.segments = buildSegments(region,numSegments);
				this.action = action;
				this.rollForwardRowThreshold = rollForwardRowThreshold;
				this.rollForwardTransactionThreshold = rollForwardTransactionThreshold;
        this.status = status;

				/*
				 * We want to periodically force the a roll forward
				 */
				rollForwardScheduler.schedule(new Runnable() {
						@Override
						public void run() {
								if(region.isClosed()||region.isClosing()){
										/*
										 * The region is closing, we don't want to try roll forwards any more
										 */
										return;
								}
								/*
								 * Select the segment with the largest number of rows to resolve,
								 * and submit it
								 */
								RegionSegment maxSegment = null;
								long maxSize = 0;
								for (RegionSegment segment : segments) {
										long toResolveCount = segment.getToResolveCount();
										if(segment.isInProgress()) continue; //skip segments which are currently still resolving
                    if(toResolveCount<=10) continue; //skip segments which have basically nothing to resolve
										if (toResolveCount > maxSize) {
												maxSegment = segment;
												maxSize = toResolveCount;
										}
								}
								try{
								/*
								 * If no rows have changed on this region, then do nothing
								 */
										if (maxSegment == null) return;

										//submit an action for processing
                    if(LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG,"Submitting task on segment %s as it has the largest size of %d",maxSegment,maxSize);
                    maxSegment.markInProgress();
                    action.submitAction(region, maxSegment.getRangeStart(), maxSegment.getRangeEnd(), new Context(maxSegment,status));
								}finally{
										//reschedule us for future execution
										rollForwardScheduler.schedule(this,10l,TimeUnit.SECONDS);
								}
						}
				}, 10l, TimeUnit.SECONDS); //force a submission every 10 seconds
		}

		private RegionSegment[] buildSegments(HRegion region, int numSegments) {
				/*
				 * Build some equi-width segments which (theoretically) cover the possible row keys
				 * for the region.
				 *
				 * This implementation is by necessity pretty poor--because we have no actual statistics
				 * as to the distribution of row keys, we essentially pick boundaries at random. This means
				 * that some segments will have lots of rows, and some segments will never be seen. C'est la vie.
				 *
				 * The strategy is to repeatedly subdivide the ranges until we reach the number of sugments specified.
				 */
				List<Pair<byte[],byte[]>> endPoints = split(region.getStartKey(),region.getEndKey(),numSegments);

				RegionSegment[] segments = new RegionSegment[endPoints.size()];
				//e is in sorted order, so just make an entry
				for(int i=0;i<endPoints.size();i++){
						Pair<byte[],byte[]> ePoints = endPoints.get(i);
						segments[i] = new RegionSegment(ePoints.getFirst(),ePoints.getSecond());
				}
				return segments;
		}

		@Override
		public void submitForResolution(byte[] rowKey, long txnId) {
        status.rowWritten();
				RegionSegment regionSegment = getSegment(rowKey,0,segments.length);
				regionSegment.update(txnId, 1l);

				long toResolveCount = updateCount.incrementAndGet();
				if((toResolveCount & segmentCheckInterval)==0){
						checkAndSubmit(regionSegment);
				}
		}



		@Override
		public void submitForResolution(ByteSlice rowKey, long txnId) {
        status.rowWritten();
				RegionSegment regionSegment = getSegment(rowKey,segments.length/2);
				regionSegment.update(txnId, 1l);

				long toResolveCount = updateCount.incrementAndGet();
				if((toResolveCount & segmentCheckInterval)==0){
						checkAndSubmit(regionSegment);
				}
		}


		@Override
		public void recordResolved(ByteSlice rowKey, long txnId) {
				RegionSegment regionSegment = getSegment(rowKey,segments.length/2);
				regionSegment.rowResolved(); //mark it resolved so that we keep track
		}

		/*****************************************************************************************************************/
		/*private helper methods*/

		private static List<Pair<byte[],byte[]>> split(byte[] start, byte[] stop, int numSegments){
				int s =1;
				int depth=0;
				while(s<numSegments){
						s<<=1;
						depth++;
				}
				List<Pair<byte[],byte[]>> endPoints = Lists.newArrayListWithExpectedSize(numSegments);
				Pair<byte[],byte[]> all = Pair.newPair(start,stop);
				endPoints.add(all);
				split(all,0,0,depth,endPoints);

				/*
				 * Splits can result in having byte[]s of different sizes. We address this issue by
				 * pre-pending 0s to make all the byte arrays the same length.
				 * There
				 * can also be ranges which are empty (e.g. [1,1)). We remove the empties.
				 */
				int maxLength = 0;
				Iterator<Pair<byte[],byte[]>> iter = endPoints.iterator();
				while(iter.hasNext()){
						Pair<byte[],byte[]> range = iter.next();
						if(Bytes.equals(range.getFirst(), range.getSecond())){
								iter.remove();
						}else{
								if(range.getFirst().length>maxLength)
										maxLength = range.getFirst().length;
								if(range.getSecond().length>maxLength)
										maxLength = range.getSecond().length;
						}
				}


				for(Pair<byte[],byte[]> range:endPoints){
						byte[] first = range.getFirst();
						range.setFirst(adjustSize(maxLength, first));
						range.setSecond(adjustSize(maxLength,range.getSecond()));
				}
				return endPoints;
		}

		private static byte[] adjustSize(int maxLength, byte[] bytes) {
				if(bytes.length==0) return bytes; //ignore the empty array
				if(bytes.length<maxLength){
						byte[] newBytes = new byte[maxLength];
						//stick the other bytes at the end
						System.arraycopy(bytes, 0, newBytes, maxLength - bytes.length, bytes.length);
						bytes = newBytes;
				}
				return bytes;
		}

		private static  void split(Pair<byte[],byte[]> toSplit,int position,int depth,int maxDepth,List<Pair<byte[],byte[]>> endPoints){
				/*
				 * Repeatedly split the range until subranges until maxDepth are arrived at.
				 * We split by choosing the "midPoint". The MidPoint is constructed by encoding the
				 * byte[] as a BigInteger using the formula
				 * sum(b[i]*(256^(length-i)).
				 */
				if(depth>=maxDepth) return; //we are done

				byte[] start = toSplit.getFirst();
				byte[] stop = toSplit.getSecond();
				BigInteger midPoint = getMidPoint(start, stop);

				byte[] newMidBytes = decodeAsBytes(midPoint);
				if(newMidBytes.length<=0) return; //we can't go any further
				Pair<byte[],byte[]> newPair = Pair.newPair(toSplit.getFirst(),newMidBytes);
				toSplit.setFirst(newMidBytes); //set the upper side
				endPoints.add(position,newPair);

				split(newPair,position,depth+1,maxDepth,endPoints);
				split(toSplit,endPoints.indexOf(toSplit),depth+1,maxDepth,endPoints);
		}

		private static BigInteger getMidPoint(byte[] start, byte[] stop) {
				BigInteger midPoint;
				if(start.length==0){
						if(stop.length==0){
								midPoint = BigInteger.valueOf(128); //128 is the halfway point
						}else{
								midPoint = encodeAsBigInteger(stop).shiftRight(1);
						}
				}else if(stop.length==0){
						BigInteger startEncoded = encodeAsBigInteger(start);
						midPoint = startEncoded.shiftRight(1).add(startEncoded);
				}else{
						midPoint = encodeAsBigInteger(stop)
										.add(encodeAsBigInteger(start)).shiftRight(1);
				}
				return midPoint;
		}

		private static byte[] decodeAsBytes(BigInteger midPoint) {
				int size = 0;
				byte[] bytes = new byte[10];
				while(midPoint.signum()>0){
						BigInteger[] byteVal = midPoint.divideAndRemainder(shift);
						bytes[size] = (byte)(byteVal[1].longValue());
						size++;
						if(size==bytes.length)
								bytes = Arrays.copyOf(bytes,3*bytes.length/2);
						midPoint  = byteVal[0];
				}

				byte[] finalBytes = Arrays.copyOf(bytes,size);
				//reverse the bytes

				for(int i=0;i<size;i++){
						finalBytes[size-i-1] = bytes[i];
				}
				return finalBytes;
		}

		private static final BigInteger shift = BigInteger.valueOf(256);
		private static BigInteger encodeAsBigInteger(byte[] val) {
				BigInteger startValue = BigInteger.ZERO;
				for(int i=val.length-1;i>=0;i--){
						BigInteger v = BigInteger.valueOf((val[i] & 0xFF));
						BigInteger term = shift.pow(val.length-1-i).multiply(v);
						startValue = startValue.add(term);
				}
				return startValue;
		}


		private RegionSegment getSegment(ByteSlice rowKey, int pos) {
				RegionSegment segment = segments[pos];
				int p = segment.position(rowKey);
				if(p==0) return segment;
				else if(p<0) return getSegment(rowKey,pos-pos/2);
				else return getSegment(rowKey,pos+pos/2);
		}

		private RegionSegment getSegment(byte[] rowKey, int start,int stop) {
        int pos = (stop+start)/2;
				RegionSegment seg = segments[pos];
				int p = seg.position(rowKey);
				if(p==0)
						return seg;
        if(pos==start||pos==stop) return seg; //this is as close are we're going to get--should never happen, but just in case
        if(p<0) return getSegment(rowKey,start,pos);
        else return getSegment(rowKey, pos, stop);
		}

		private void checkAndSubmit(RegionSegment regionSegment) {
				if(regionSegment.isInProgress()) return; //don't submit more than one resolve action at a time
				/*
				 * We only want to submit a segment for rolling forward
				 * if the following 2 criteria are met:
				 *
				 * 1. There are a considerable number of rows to roll forward (e.g. > rollForwardRowThreshold)
				 * 2. There are a considerable number of committed transactions which wrote to this segment.
				 *
				 * We have no way of knowing for sure that criteria #2 is ever true, but we can use a heuristic
				 * to guess--if the number of transactions which wrote to this segment is high (e.g. > rollForwardTransactionThreshold),
				 * then it is likely that we have some committed/rolled back transactions.
				 */
				long numRows = regionSegment.getToResolveCount();
				if(numRows>rollForwardRowThreshold){
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"segment %s exceeds row threshold",regionSegment);
						long uniqueTxns = (long)regionSegment.estimateUniqueWritingTransactions();
						if(uniqueTxns>rollForwardTransactionThreshold){
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG,"segment %s has %d transactions, exceeding the threshold. Rolling forward",regionSegment,uniqueTxns);
								//we've exceeded our thresholds, a RollForward action is a good option now
								regionSegment.markInProgress();
								action.submitAction(region,regionSegment.getRangeStart(),regionSegment.getRangeEnd(),new Context(regionSegment,status));
						}
				}
		}}
