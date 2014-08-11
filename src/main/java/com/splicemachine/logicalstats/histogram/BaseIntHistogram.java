package com.splicemachine.logicalstats.histogram;

import com.google.common.primitives.Ints;

import java.util.List;

/**
 * Base class to make implementing Int histograms simpler.
 * @author Scott Fines
 * Date: 4/1/14
 */
public abstract class BaseIntHistogram implements IntHistogram {

		private final long totalElements;

		public BaseIntHistogram(long totalElements) {
				this.totalElements = totalElements;
		}

		@Override
		public long after(int value, boolean equals) {
				int max = max();
				if(value > max ||(value ==max &&!equals)) return 0;

				IntBucket b = minBucket();
				IntBucket n = next(b);
				while(n!=null && n.startValue<=value){
						b = n;
						n = next(n);
				}
				long total = b.estimateGreaterThan(value,equals);
				b = n;
				while(b!=null){
						total+=b.numElements;
						b = next(b);
				}
				return total;
		}

		@Override
		public long errorAfter(int value, boolean equals) {
				IntBucket b = minBucket();
				IntBucket n = next(b);
				while(n!=null && n.startValue<value){
						b = n;
						n = next(n);
				}
				long estimate = b.estimateGreaterThan(value,equals);
				long max = b.numElements;
				return Math.max(estimate,max-estimate);
		}

		@Override
		public long before(int value, boolean equals) {
				if(value<min()) return 0l;
				long numElements = 0;
				IntBucket b = minBucket();
				IntBucket n = next(b);
				while(n!=null && n.startValue<=value){
						numElements+=b.numElements;
						b = n;
						n = next(n);
				}
				numElements+=b.estimateLessThan(value,equals);
				return numElements;
		}

		@Override
		public long errorBefore(int value, boolean equals) {
				IntBucket b = seekTo(value);

				long count = b.estimateLessThan(value,equals);
				long max = b.numElements;
				return Math.max(count,max-count);
		}

		protected IntBucket seekTo(int value) {
				IntBucket b = minBucket();
				IntBucket n = next(b);
				while(n!=null && n.startValue<=value){
						b = n;
						n = next(b);
				}
				return b;
		}

		@Override
		public long equal(int value) {
				IntBucket b = seekTo(value);
				return b.estimateEquals(value);
		}

		@Override
		public long errorEqual(int value) {
				IntBucket b = seekTo(value);
				long count = b.estimateEquals(value);
				return Math.max(count,b.numElements-count);
		}

		@Override
		public long between(int startValue, int endValue, boolean inclusiveStart, boolean inclusiveEnd) {
				if(startValue<min()){
						if(endValue>max()) return totalElements;
						else return before(endValue,inclusiveEnd);
				}else if(endValue>max())
						return after(startValue,inclusiveStart);

				IntBucket sb = seekTo(startValue);

				IntBucket eb = sb;
				IntBucket n = next(eb);
				if(n==null||((!inclusiveEnd && n.startValue>=endValue)||(inclusiveEnd && n.startValue>endValue))){
						return eb.estimateLessThan(endValue,inclusiveEnd)-sb.estimateLessThan(startValue,!inclusiveStart);
				}

				eb = n;
				n = next(n);
				long numElements =0;
				while(n!=null && n.startValue<endValue){
						numElements+=eb.numElements;
						eb = n;
						n = next(n);
				}

				numElements+=sb.estimateGreaterThan(startValue,inclusiveStart);
				numElements+=eb.estimateLessThan(endValue,inclusiveEnd);
				return numElements;
		}

		@Override
		public long errorBetween(int startValue, int endValue, boolean inclusiveStart, boolean inclusiveEnd) {
/*
				 * Estimate the maximum estimation error caused by interpolation.  In this case, it's the
				 * error of interpolating >(=) start and interpolating <(=) end
				 */
				IntBucket sb = seekTo(startValue);

				IntBucket eb = sb;
				IntBucket n = next(eb);
				if(n==null||((!inclusiveEnd && n.startValue>=endValue)||(inclusiveEnd && n.startValue>endValue))){
						long total = eb.estimateLessThan(endValue,inclusiveEnd)-sb.estimateLessThan(startValue,!inclusiveStart);
						return Math.max(total,eb.numElements-total);
				}
				eb = n;
				n = next(eb);
				while(n!=null && n.startValue<endValue){
						eb = n;
						n = next(n);
				}

				long total = sb.estimateGreaterThan(startValue,inclusiveStart);
				long totError = Math.max(total,sb.numElements-total);
				total = eb.estimateLessThan(endValue,inclusiveEnd);
				totError+=Math.max(total,eb.numElements-total);
				return totError;
		}

		/*Autoboxing methods*/
		@Override public Integer getMin() { return min(); }
		@Override public Integer getMax() { return max(); }
		@Override public List<? extends Column<Integer>> getColumns() { return columns(); }

		@Override
		public long getNumElements(Integer start, Integer end, boolean inclusiveStart, boolean inclusiveEnd) {
				if(start==null || start<min()){
						if(end==null || end > max()) return totalElements;
						return before(end,inclusiveEnd);
				}else if(end==null || end> max())
						return after(start,inclusiveStart);
				else if(end.equals(start)){
						if(inclusiveStart && inclusiveEnd)
								return equal(start);
						return 0l;
				}else
						return between(start,end,inclusiveStart,inclusiveEnd);
		}

		@Override
		public long maxError(Integer start, Integer end, boolean inclusiveStart, boolean inclusiveEnd) {
				if(start==null || start<min()){
						if(end==null || end > max()) return 0;
						return errorBefore(end,inclusiveEnd);
				}else if(end==null || end> max())
						return errorAfter(start,inclusiveStart);
				else if(end.equals(start)){
						if(inclusiveStart && inclusiveEnd)
								return errorEqual(start);
						return 0l;
				}else
						return errorBetween(start,end,inclusiveStart,inclusiveEnd);
		}

		@Override
		public String toString() {
				StringBuilder sb = new StringBuilder("[");
				IntBucket b = minBucket();
				while(b!=null){
						sb = sb.append(b);
						b = next(b);
						if(b!=null)
								sb = sb.append(",");
				}
				sb = sb.append("]");
				return sb.toString();
		}

		protected abstract IntBucket minBucket();

		protected abstract IntBucket next(IntBucket start);

		protected class LowInterpolatedBucket extends InterpolatedBucket{

				private final long elementsAfterStop;
				private final long elementsEqualStop;
				private final int stop;

				public LowInterpolatedBucket(IntBucket bucket, int start, int stop) {
						super(bucket, start);
						this.stop = stop;

						this.elementsAfterStop = bucket.estimateGreaterThan(stop,false);
						this.elementsEqualStop = bucket.estimateEquals(stop);
				}

				@Override
				public long estimateGreaterThan(int element, boolean inclusive) {
						return super.estimateGreaterThan(element, inclusive)-elementsAfterStop-elementsEqualStop;
				}

				@Override
				public long getNumElements() {
						return super.getNumElements()-elementsAfterStop-elementsEqualStop;
				}

				@Override
				public long estimateLessThan(int element, boolean inclusive) {
						if(element>=stop)
								return getNumElements();
						return super.estimateLessThan(element, inclusive);
				}

				@Override
				public long estimateEquals(int element) {
						if(element>stop) return 0l;
						if(element==stop) return elementsEqualStop;
						return super.estimateEquals(element);
				}

				@Override
				public IntColumn lowInterpolatedColumn(int element) {
						if(element>=stop) return this;
						return new LowInterpolatedBucket(bucket,start,element);
				}

				@Override
				public IntColumn highInterpolatedColumn(int element) {
						if(element>=stop) return null;
						return super.highInterpolatedColumn(element);
				}

				@Override
				public String toString() {
						return "{"+leastElement()+":"+getNumElements()+"}L";
				}
		}

		protected class InterpolatedBucket implements IntColumn{
				protected IntBucket bucket;

				protected int start;

				private final long elementsBeforeStart;
				private final long elementsEqualStart;


				public InterpolatedBucket(IntBucket bucket,int start) {
						this.bucket = bucket;
						this.start =start;
						this.elementsBeforeStart = bucket.estimateLessThan(start, false);
						this.elementsEqualStart = bucket.estimateEquals(start);
				}

				@Override public int leastElement() { return start; }

				@Override
				public long estimateLessThan(int element, boolean inclusive) {
						if(element<start) return 0l;
						return bucket.estimateLessThan(element,inclusive) - elementsBeforeStart;
				}

				@Override
				public long estimateGreaterThan(int element, boolean inclusive) {
						if(element<=start)
								return getNumElements();

						return bucket.estimateGreaterThan(element,inclusive);
				}

				@Override
				public long estimateEquals(int element) {
						if(element<start) return 0l;
						else if(element==start) return elementsEqualStart;
						else
								return bucket.estimateEquals(element);
				}

				@Override
				public IntColumn lowInterpolatedColumn(int element) {
						if(start>=element) return null;
						return new LowInterpolatedBucket(bucket,start,element);
				}

				@Override
				public IntColumn highInterpolatedColumn(int element) {
						if(element<=start) return this;
						return new InterpolatedBucket(bucket,element);
				}

				@Override public Integer getLeastElement() { return start; }

				@Override
				public long getNumElements() {
						return bucket.numElements-elementsBeforeStart;
				}

				@Override
				public long estimateLessThan(Integer element, boolean inclusive) {
						assert element!=null : "Null integer not allowed";
						return estimateLessThan(element.intValue(),inclusive);
				}

				@Override
				public long estimateGreaterThan(Integer element, boolean inclusive) {
						assert element!=null : "Null integer not allowed";
						return estimateGreaterThan(element.intValue(),inclusive);
				}

				@Override
				public long estimateEquals(Integer element) {
						assert element!=null : "Null integer not allowed";
						return estimateEquals(element.intValue());
				}

				@Override
				public Column<Integer> lowInterpolatedColumn(Integer element) {
						assert element!=null : "Null integer not allowed";
						return lowInterpolatedColumn(element.intValue());
				}

				@Override
				public Column<Integer> highInterpolatedColumn(Integer element) {
						assert element!=null : "Null integer not allowed";
						return highInterpolatedColumn(element.intValue());
				}

				@Override public int compareTo(Column<Integer> o) { return Ints.compare(start,o.getLeastElement()); }

				@Override public String toString() { return "{"+leastElement()+":"+getNumElements()+"}H"; }
		}
		protected abstract class IntBucket implements IntColumn{
				protected int startValue;
				protected long numElements;

				@Override public int leastElement() { return startValue; }
				@Override public Integer getLeastElement() { return startValue; }
				@Override public long getNumElements() { return numElements; }
				//todo -sf- autoboxing?
				@Override public int compareTo(Column<Integer> o) { return Ints.compare(startValue,o.getLeastElement()); }

				@Override
				public String toString() {
						return "{"+startValue+":"+numElements+"}";
				}

				public long estimateLessThan(int value, boolean inclusive){
						if(value==startValue){
								if(inclusive) return estimateEquals(value);
								else return 0;
						}

						IntBucket nextBucket = next(this);
						int stop;
						if(nextBucket==null){
								stop = max();
								if(stop==value){
										return inclusive? numElements : numElements-1;
								}
						}else
								stop = nextBucket.startValue;

						long lessThan = interpolate(startValue,stop,value,numElements);
						if(inclusive)
								lessThan+=estimateEquals(value);
						return lessThan;
				}

				private long interpolate(int start, int stop, int element, long totalElements) {
						int run = stop-start;
						double slope = ((double)totalElements)/run;
						return (long)(slope*(element-start));
				}

				public long estimateGreaterThan(int value, boolean inclusive){
						return numElements - estimateLessThan(value,!inclusive);
				}

				@Override
				public long estimateLessThan(Integer element, boolean inclusive) {
						if(element==null) return numElements;
						return estimateLessThan(element.intValue(),inclusive);
				}

				@Override
				public long estimateGreaterThan(Integer element, boolean inclusive) {
						if(element==null) return numElements;
						return estimateGreaterThan(element.intValue(),inclusive);
				}

				@Override
				public long estimateEquals(Integer element) {
						if(element==null) return 0l;
						//noinspection UnnecessaryUnboxing
						return estimateEquals(element.intValue());
				}

				@Override
				public IntColumn lowInterpolatedColumn(int element) {
						return new LowInterpolatedBucket(this,startValue,element);
				}

				@Override
				public IntColumn highInterpolatedColumn(int element) {
						if(element<startValue) return this;
						return new InterpolatedBucket(this,element);
				}

				@Override
				public Column<Integer> lowInterpolatedColumn(Integer element) {
						assert element!=null: "Nulls integers not allowed";
						return lowInterpolatedColumn(element.intValue());
				}

				@Override
				public Column<Integer> highInterpolatedColumn(Integer element) {
						assert element!=null: "Nulls integers not allowed";
						return highInterpolatedColumn(element.intValue());
				}
		}
}
