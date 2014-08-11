package com.splicemachine.logicalstats.histogram;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 4/1/14
 */
public class EquiDepthIntHistogram extends BaseIntHistogram {

		private EquiBucket minBucket;
		private int max;
		private int numBuckets;

		EquiDepthIntHistogram(int[] elements,int numBuckets) {
				super(elements.length);

				this.numBuckets = numBuckets;

				constructHistogram(elements,numBuckets);
		}


		@Override public int min() { return minBucket.startValue; }
		@Override public int max() { return max; }

		@Override
		public List<IntColumn> columns() {
				List<IntColumn> columns = Lists.newArrayListWithCapacity(numBuckets);
				EquiBucket b = minBucket;
				while(b!=null){
						columns.add(b);
						b = b.next;
				}

				return columns;
		}

		@Override public int getNumBuckets() { return numBuckets; }
		@Override protected IntBucket minBucket() { return minBucket; }
		@Override protected IntBucket next(IntBucket start) { return ((EquiBucket)start).next; }

		private class EquiBucket extends IntBucket{
				private int cardinality;
				private int numStartElements;

				private EquiBucket next;

				@Override
				public long estimateEquals(int value) {
						if(value==startValue) return numStartElements;
						return (int)Math.ceil(((float)(numElements-numStartElements)/(cardinality)));
				}

		}

		private void constructHistogram(int[] elements, int numBuckets) {
				Arrays.sort(elements);
				this.max = elements[elements.length-1];

				int min = elements[0];
				EquiBucket startBucket = new EquiBucket();
				startBucket.numElements = elements.length;
				startBucket.numStartElements = countEquals(elements, 0, elements.length, min);

				minBucket = startBucket;
				this.numBuckets = split(elements,minBucket,0,elements.length,1,numBuckets);

				//get cardinalities
				EquiBucket b = minBucket;
				EquiBucket n = b.next;
				int previous = elements[0];
				int cardinality = 1;
				for(int i=1;i<elements.length;i++){
						int next = elements[i];
						if(previous!=next){
								previous = next;
								if(n!=null &&n.startValue<=next){
										b.cardinality = cardinality;
										b = n;
										n = n.next;
										cardinality=0;
								}else{
										cardinality++;
								}
						}
				}
				b.cardinality = cardinality;
		}

		private int split(int[] elements, EquiBucket toSplit,
											int startPosition,
											int stopPosition,
											int numBucketsCreated,
											int maxBuckets) {
				if(maxBuckets<=1) return numBucketsCreated;
				if(startPosition>=stopPosition) return numBucketsCreated;

				int rightElementCount = (stopPosition-startPosition)/2;

				if(rightElementCount<=0) return numBucketsCreated;
				int medianPos = startPosition+rightElementCount;
				if((stopPosition-startPosition)%2!=0)
						rightElementCount++;
				int median = elements[medianPos];
				while(medianPos<stopPosition && elements[medianPos-1]==median){
						medianPos++;
						rightElementCount--;
						median = elements[medianPos];
				}

				boolean recurseRight = true;
				if(rightElementCount<=0){
						/*
						 * All elements to the right of the median are all the same element. We need to go left from the median
						 * position and try to make the right bigger. If that doesn't work, then everything matches start
						 * and we are as small as we can go.
						 */
						recurseRight=false;
						rightElementCount = (stopPosition- startPosition)/2;
						medianPos = startPosition+rightElementCount;
						if((stopPosition-startPosition)%2!=0)
								rightElementCount++;
						median = elements[medianPos];
						while(medianPos>=startPosition && elements[medianPos-1]==median){
								medianPos--;
								rightElementCount++;
								median = elements[medianPos];
						}
						if(medianPos<startPosition){
								//all elements in this bucket are the same, do nothing
								return numBucketsCreated;
						}
				}

				//we've found the split point for the bucket, now construct the split
				EquiBucket newBucket = new EquiBucket();
				newBucket.startValue = median;
				newBucket.numElements = rightElementCount;
				newBucket.numStartElements = countEquals(elements,medianPos,stopPosition,median);
				newBucket.next = toSplit.next;
				toSplit.numElements-=rightElementCount;
				toSplit.next = newBucket;

				numBucketsCreated = split(elements,toSplit,startPosition,medianPos,numBucketsCreated+1,maxBuckets/2);
				if(recurseRight)
						numBucketsCreated = split(elements, newBucket, medianPos, stopPosition, numBucketsCreated, maxBuckets/2);

				return numBucketsCreated;
		}

		private int countEquals(int[] elements, int start, int stop, int value) {
				int equals = 0;
				for(int i=start;i<stop;i++){
						if(elements[i]!=value)
								return equals;
						else
								equals++;
				}
				return equals;
		}

		public static void main(String...args) throws Exception{
				int size = 1024;
				IntArrayList data = IntArrayList.newInstanceWithCapacity(size);

				for(int i=0;i<size;i++){
						data.add(i);
						if(i%2==0)
								data.add(i);
//						if(i==479){
//								for(int j=0;j<376;j++){
//										data.add(j);
//								}
//						}
				}
				int[] toAdd = data.toArray();
				IntHistogram histogram = new EquiDepthIntHistogram(toAdd,16);
				System.out.println(histogram);

				System.out.printf("total:%d%n",histogram.between(Integer.MIN_VALUE,Integer.MAX_VALUE, false, false));
				System.out.printf("<0:%d%n",histogram.before(0,false));
				System.out.printf("<=0:%d%n",histogram.before(0,true));
				System.out.printf("<100:%d%n",histogram.before(100,false));
				System.out.printf("<=100:%d%n",histogram.getNumElements(null,100,false,true));
				System.out.printf("<=100:%d%n",histogram.before(100,true));
				System.out.printf(">100:%d%n",histogram.after(100,false));
				System.out.printf(">=100:%d%n",histogram.after(100,true));
				System.out.printf("=100:%d%n",histogram.equal(100));
				System.out.println();
//				System.out.printf("<476:%d%n",histogram.getNumElements(null,476,false,false));
//				System.out.printf("<=476:%d%n",histogram.getNumElements(null,476,false,true));
//				System.out.printf(">476:%d%n",histogram.getNumElements(476,null,false,false));
//				System.out.printf(">=476:%d%n",histogram.getNumElements(476,null,true,false));
//				System.out.printf("=476:%d%n",histogram.getNumElements(476,476,true,true));
//				System.out.println();
//				System.out.printf("<479:%d%n",histogram.getNumElements(null,479,false,false));
//				System.out.printf("<=479:%d%n",histogram.getNumElements(null,479,false,true));
//				System.out.printf(">479:%d%n",histogram.getNumElements(479,null,false,false));
//				System.out.printf(">=479:%d%n",histogram.getNumElements(479,null,true,false));
//				System.out.printf("=479:%d%n",histogram.getNumElements(479,479,true,true));
//				System.out.printf("<1000:%d%n",histogram.getNumElements(null,1000));
//				System.out.printf(">=1000:%d%n",histogram.getNumElements(1000,null));
//				System.out.printf(">100 && < 1000:%d%n",histogram.getNumElements(100,1000));
		}
}
