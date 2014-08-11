package com.splicemachine.logicalstats.histogram;

import com.google.common.collect.Lists;
import com.splicemachine.utils.ComparableUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Useful histogram for implementing a histogram over generic comparable objects. For primitive types,
 * it is more efficient to use the primitive-specific Histogram constructions,as this implementation will
 * not avoid autoboxing.
 *
 * @author Scott Fines
 * Date: 3/31/14
 */
public abstract class BaseObjectHistogram<T extends Comparable<T>> implements Histogram<T>{

		//we keep the bucket list as a doubly-linked list of buckets
		private Bucket minBucket;

		T maxElement;

		private int numBuckets;
		private int totalElements;

		protected BaseObjectHistogram(T[] buffer, int numBuckets){
				assert buffer!=null: "Cannot build a histogram with a null buffer!";
				assert buffer.length>0: "Require at least one element in the buffer!";

				constructHistogram(buffer,numBuckets);
		}

		@Override public T getMin() { return minBucket.startValue; }
		@Override public T getMax() { return maxElement; }

		@Override
		public List<Column<T>> getColumns() {
				return null;
		}

		@Override public int getNumBuckets() { return numBuckets; }

		@Override
		public long getNumElements(T start, T end, boolean inclusiveStart, boolean inclusiveEnd) {
				if(start==null){
						if(end==null) return totalElements;
						return lessThan(end,inclusiveEnd);
				}else if(end==null)
						return greaterThan(start,inclusiveStart);
				else if(start.compareTo(end)==0){
						if(inclusiveStart && inclusiveEnd)
								return equal(start);
						else return 0;
				}else
						return between(start,end,inclusiveStart,inclusiveEnd);
		}

		@Override
		public long maxError(T start, T end, boolean inclusiveStart,boolean inclusiveEnd) {
				if(start==null || minBucket.startValue.compareTo(start)>0){
						if(end==null || maxElement.compareTo(end)<0) return 0l;
						return errorLessThan(end,inclusiveEnd);
				}else if(end==null || maxElement.compareTo(end)<0){
						return errorGreaterThan(start,inclusiveStart);
				}else if(start.compareTo(end)==0){
						if(inclusiveStart && inclusiveEnd)
								return errorEquals(start);
						else
								return 0l;
				}else{
						return errorBetween(start,end,inclusiveStart,inclusiveEnd);
				}
		}

		@Override
		public String toString() {
				StringBuilder sb = new StringBuilder("[");
				Bucket b = minBucket;
				while(b!=null){
						sb = sb.append(b);
						b = b.next;
						if(b!=null)
								sb = sb.append(",");
				}
				sb = sb.append("]");
				return sb.toString();
		}

		protected abstract int interpolate(T start, T stop, T element,int totalElements);

		protected class Bucket implements Column<T>{
				protected T startValue;
				protected int numStartValues; // the number of entries which match the start key
				protected int numElements; // the total number of elements in the bucket (including start values).
				protected int numUniqueElements;

				/*A link to the next bucket in the list*/
				private Bucket next;

				protected Bucket(){}

				private Bucket(T t) {
						startValue = t;
						numStartValues=1;
						numElements=1;
				}

				@Override public String toString() { return "{"+startValue+":("+numElements+","+numStartValues+")}"; }
				@Override public T getLeastElement() { return startValue; }
				@Override public long getNumElements() { return numElements; }
				@Override public int compareTo(Column<T> o) { return startValue.compareTo(o.getLeastElement()); }

				public long estimateGreaterThan(T element,boolean inclusive) {
						/*
						 * we are in the middle of the bucket, so use linear interpolation to estimate the number
						 */
						return numElements - estimateLessThan(element,!inclusive);
				}

				public long estimateEquals(T start) {
						if(startValue.compareTo(start)==0) return numStartValues;

						return (int)Math.ceil(((float)(numElements-numStartValues)/(numUniqueElements-1)));
				}

				/**
				 * @param element an element guaranteed to be >= start of this bucket.
				 * @return the estimated number of elements that fall between this bucket's start and {@code next}
				 */
				public long estimateLessThan(T element,boolean inclusive){
						if(element.compareTo(startValue)==0){
								if(inclusive)
										return estimateEquals(element);
								else
										return 0;
						}

						/*
						 * We are in the middle of the bucket, so use linear interpolation to estimate the number
						 */
						T stop = next!=null? next.startValue: maxElement;
						int lessThan = interpolate(startValue,stop,element,numElements);
						if(inclusive)
								lessThan+=estimateEquals(startValue);
						return lessThan;
				}

				@Override
				public Column<T> lowInterpolatedColumn(T element) {
						throw new UnsupportedOperationException("Implement!");
				}

				@Override
				public Column<T> highInterpolatedColumn(T element) {
						throw new UnsupportedOperationException("Implement!");
				}
		}

		/********************************************************************************************************************/
		/*private helper functions*/

		/*Functions for estimating between range queries*/
		private long between(T start, T end,boolean inclusiveStart, boolean inclusiveEnd) {
				Bucket sb = minBucket;
				Bucket n = sb.next;
				while(n!=null && n.startValue.compareTo(start)<0){
						sb = n;
						n = n.next;
				}

				Bucket eb = sb;
				n = eb.next;
				if(n==null){
						//we are at the end of the histogram, so this bucket covers everything
						return eb.estimateLessThan(end,inclusiveEnd)-sb.estimateLessThan(start,!inclusiveStart);
				}else if(n.startValue.compareTo(end)>0){
						//the next bucket starts past our end point
						return eb.estimateLessThan(end,inclusiveEnd)-sb.estimateLessThan(start,!inclusiveStart);
				}else if(n.startValue.compareTo(end)==0 && !inclusiveEnd){
						return sb.getNumElements();
//						return eb.estimateLessThan(end,inclusiveEnd)-sb.estimateLessThan(start,!inclusiveStart);
				}

				eb = n;
				n = eb.next;
				int numElements=0;
				while(n!=null && n.startValue.compareTo(end)<0){
						numElements+=eb.numElements;
						eb = n;
						n = n.next;
				}

				numElements+=sb.estimateGreaterThan(start,inclusiveStart);
				numElements+=eb.estimateLessThan(end,inclusiveEnd);

				return numElements;
		}

		private long errorBetween(T start, T end, boolean inclusiveStart, boolean inclusiveEnd) {
				/*
				 * Estimate the maximum estimation error caused by interpolation.  In this case, it's the
				 * error of interpolating >(=) start and interpolating <(=) end
				 */
				Bucket sb = minBucket;
				Bucket n = sb.next;
				while(n!=null && n.startValue.compareTo(start)<0){
						sb = n;
						n = n.next;
				}

				Bucket eb = sb;
				n = eb.next;
				if(n==null || n.startValue.compareTo(end)>=0){
						long total = eb.estimateLessThan(end,inclusiveEnd)-sb.estimateLessThan(start,!inclusiveStart);
						return Math.max(total,eb.numElements-total);
				}
				eb = n;
				n = eb.next;
				while(n!=null && n.startValue.compareTo(end)<0){
						eb = n;
						n = n.next;
				}

				long total = sb.estimateGreaterThan(start,inclusiveStart);
				long totError = Math.max(total,sb.numElements-total);
				total = eb.estimateLessThan(end,inclusiveEnd);
				totError+=Math.max(total,eb.numElements-total);
				return totError;
		}

		/*Methods for estimating greater-than*/
		private long greaterThan(T element,boolean inclusiveEnd) {
				Bucket b = minBucket;
				Bucket n = minBucket.next;
				while(n!=null &&n.startValue.compareTo(element)<0){
						b = n;
						n = n.next;
				}
				if(b==null) return 0;

				long numElements = b.estimateGreaterThan(element,inclusiveEnd);
				b = b.next;
				while(b!=null){
						numElements+=b.numElements;
						b = b.next;
				}
				return numElements;
		}

		private long errorGreaterThan(T start, boolean inclusiveStart){
				Bucket b = minBucket;
				Bucket n = b.next;
				while(n!=null && n.startValue.compareTo(start)<0){
						b = n;
						n = n.next;
				}

				long count = b.estimateGreaterThan(start, inclusiveStart);
				int max = b.numElements;
				return Math.max(count,max-count);
		}

		/*Methods for estimating <(=) queries*/
		private long lessThan(T end,boolean inclusive) {
				int numElements = 0;
				Bucket b = minBucket;
				Bucket n = b.next;
				while(n!=null && n.startValue.compareTo(end)<0){
						numElements+=b.numElements;
						b = n;
						n = n.next;
				}

				//estimate any boundary effort
				numElements+=b.estimateLessThan(end,inclusive);
				return numElements;
		}

		private long errorLessThan(T end, boolean inclusiveEnd) {
				Bucket b = minBucket;
				Bucket n = b.next;
				while(n!=null && n.startValue.compareTo(end)<0){
						b = n;
						n = n.next;
				}

				long count = b.estimateLessThan(end,inclusiveEnd);
				int max = b.numElements;
				return Math.max(count,max-count);
		}

		/*Methods for estimating equality*/
		private long equal(T start) {
				Bucket b = minBucket;
				Bucket n = b.next;
				while(n!=null && n.startValue.compareTo(start)<0){
						b = n;
						n = n.next;
				}
				return b.estimateEquals(start);
		}

		private long errorEquals(T element) {
				Bucket b = minBucket;
				Bucket n = b.next;
				while(n!=null && n.startValue.compareTo(element)<0){
						b = n;
						n = n.next;
				}

				long count = b.estimateEquals(element);
				int max = b.numElements;
				return Math.max(count,max-count);
		}

		/*Methods for constructing the histogram*/
		private int splitBucket(T[] buffer,
														int startPosition,
														int endPosition,
														Bucket toSplit,
														int buckets,
														int maxBuckets) {
				/*
		 		 * Returns the number of buckets created
		 		 */
				if(maxBuckets<=1) return buckets;
				if(startPosition==endPosition) return buckets; //can't create any more buckets, we're out of items

				int rightElementCount = (endPosition-startPosition)/2;
				int medianPos = startPosition+rightElementCount;
				if((endPosition-startPosition)%2!=0)
						rightElementCount++;
				T median = buffer[medianPos];
				while(medianPos<endPosition && buffer[medianPos-1].equals(median)){
						medianPos++;
						rightElementCount--;
						median = buffer[medianPos];
				}
				boolean recurseRight = true;
				if(rightElementCount<=0){
					 /*
					  * All elements to the right of the median are the same actual element. We need to backtrack now from
					  * the median position and try and go bigger. If that doesn't work, then everything matches the start
 					  */
						recurseRight = false;
						rightElementCount = (endPosition-startPosition)/2;
						medianPos = startPosition+rightElementCount;
						if((endPosition-startPosition)%2!=0)
								rightElementCount++;
						median = buffer[medianPos];
						while(medianPos>=startPosition && buffer[medianPos-1].equals(median)){
								medianPos--;
								rightElementCount++;
								median = buffer[medianPos];
						}
						if(medianPos<startPosition){
								/*
								 * All elements in this bucket are the same, don't do anything;
								 */
								return buckets;
						}
				}

				//we've found the split point for the bucket
				Bucket newBucket = new Bucket(median);
				newBucket.numElements=rightElementCount;
				newBucket.numStartValues = numEqualsStart(buffer,medianPos,endPosition,median);
				toSplit.numElements-=rightElementCount;
				newBucket.next = toSplit.next;
				toSplit.next = newBucket;

				//split the next layer of buckets
				buckets=splitBucket(buffer,startPosition,medianPos,toSplit,buckets+1,maxBuckets/2);
				if(recurseRight)
						buckets = splitBucket(buffer,medianPos,endPosition,newBucket,buckets,maxBuckets/2);
				return buckets;
		}

		private void constructHistogram(T[] buffer, int numBuckets) {
			 /*
			  * Approach is as follows:
			  *
			  * 1. Sort the data. This is most efficient, since we're going to rely on this sort order repeatedly.
			  * 2. Choose the first unique value AFTER the median
			  * 	2a. Choose the median.
			  * 	2b. compare with element before the median.
			  * 	2c. If the same, iterate through the array until a different value is arrived at (or the end of the array is reached).
			  * 3. One bucket is the left side, one bucket is the right side.
			  * 4. Recursively apply 2-3 until numBuckets buckets have been constructed
			  *
			  * To make sure that we have a full buffer, we do a first pass looking for nulls. If there are nulls, then
			  * the nulls must be sorted
			  */
				Arrays.sort(buffer, ComparableUtils.nullsLastComparator());
				//find the first null--that's the end of our actual buffer
				int lastNonNull = buffer.length;
				while(buffer[lastNonNull-1]==null){
						lastNonNull--;
				}
				if(lastNonNull<buffer.length){
						this.maxElement = buffer[lastNonNull];
						lastNonNull++;
				}else
						this.maxElement = buffer[lastNonNull-1];

				this.totalElements = lastNonNull;


				T min = buffer[0];
				Bucket startBucket = new Bucket(min);
				startBucket.numElements = lastNonNull;
				startBucket.numStartValues = numEqualsStart(buffer, 0,lastNonNull, min);

				minBucket = startBucket;
				this.numBuckets = splitBucket(buffer, 0,lastNonNull, startBucket, 1,numBuckets);
				//get cardinalities
				Bucket b = minBucket;
				Bucket n = b.next;
				T previous = buffer[0];
				int cardinality = 1;
				for(int i=1;i<buffer.length;i++){
						T next = buffer[i];
						if(previous.compareTo(next)!=0){
								previous = next;
								if(n!=null &&n.startValue.compareTo(next)<=0){
										b.numUniqueElements = cardinality;
										b = n;
										n = n.next;
										cardinality=0;
								}else{
										cardinality++;
								}
						}
				}
				b.numUniqueElements = cardinality;
		}
		private int numEqualsStart(T[] buffer, int start,int end, T matchValue) {
				int numEqualsStart = 0;
				for(int i=start;i<end;i++){
						if(!buffer[i].equals(matchValue))
								break;
						else
								numEqualsStart++;
				}
				return numEqualsStart;
		}


		public static void main(String...args) throws Exception{
				int size = 1024;
				List<Integer> data = Lists.newArrayListWithCapacity(size);

				for(int i=0;i<size;i++){
						data.add(i);
//						if(i%2==0)
//								data.add(i);
//						if(i==479){
//								for(int j=0;j<376;j++){
//										data.add(j);
//								}
//						}
				}
				Integer[] toAdd = new Integer[size];
				toAdd = data.toArray(toAdd);
				Histogram<Integer> histogram = new BaseObjectHistogram<Integer>(toAdd,16){

						@Override
						protected int interpolate(Integer start, Integer stop,Integer element,int totalElements) {
								int run = stop-start;
								float slope = ((float)totalElements)/run;
								return (int)(slope*(element-start));
						}
				};
				System.out.println(histogram);

				System.out.printf("total:%d%n",histogram.getNumElements(null,null,false,false));
				System.out.printf("<0:%d%n",histogram.getNumElements(null,0,false,false));
				System.out.printf("<=0:%d%n",histogram.getNumElements(null,0,false,true));
				System.out.printf("<100:%d%n",histogram.getNumElements(null,100,false,false));
				System.out.printf("<=100:%d%n",histogram.getNumElements(null,100,false,true));
				System.out.printf(">100:%d%n",histogram.getNumElements(100,null,false,false));
				System.out.printf(">=100:%d%n",histogram.getNumElements(100,null,true,false));
				System.out.printf("=100:%d%n",histogram.getNumElements(100,100,true,true));
				System.out.println();
				System.out.printf("<476:%d%n",histogram.getNumElements(null,476,false,false));
				System.out.printf("<=476:%d%n",histogram.getNumElements(null,476,false,true));
				System.out.printf(">476:%d%n",histogram.getNumElements(476,null,false,false));
				System.out.printf(">=476:%d%n",histogram.getNumElements(476,null,true,false));
				System.out.printf("=476:%d%n",histogram.getNumElements(476,476,true,true));
				System.out.println();
				System.out.printf("<479:%d%n",histogram.getNumElements(null,479,false,false));
				System.out.printf("<=479:%d%n",histogram.getNumElements(null,479,false,true));
				System.out.printf(">479:%d%n",histogram.getNumElements(479,null,false,false));
				System.out.printf(">=479:%d%n",histogram.getNumElements(479,null,true,false));
				System.out.printf("=479:%d%n",histogram.getNumElements(479,479,true,true));
//				System.out.printf("<1000:%d%n",histogram.getNumElements(null,1000));
//				System.out.printf(">=1000:%d%n",histogram.getNumElements(1000,null));
//				System.out.printf(">100 && < 1000:%d%n",histogram.getNumElements(100,1000));
		}
}
