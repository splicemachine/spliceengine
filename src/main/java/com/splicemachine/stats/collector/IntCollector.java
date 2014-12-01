package com.splicemachine.stats.collector;

import com.splicemachine.stats.IntUpdateableBuilder;
import com.splicemachine.stats.cardinality.IntCardinalityEstimator;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.IntFrequencyCounter;
import com.splicemachine.stats.histogram.IntRangeQuerySolver;
import com.splicemachine.stats.order.IntMinMaxCollector;

import java.util.Set;

/**
 * Collector which accumulates Statistical information for a specific entry.
 *
 * @author Scott Fines
 * Date: 6/5/14
 */
public class IntCollector implements IntStatsCollector{
		private long totalCount = 0l;
		private long nullInts = 0l;

		private final IntCardinalityEstimator cardinalityEstimator;
		private final IntMinMaxCollector minMaxCollector;
		private final IntFrequencyCounter frequencyCounter;
		private final IntUpdateableBuilder<IntRangeQuerySolver> histogram;
		private IntRangeQuerySolver hist;

		public IntCollector(IntCardinalityEstimator cardinalityEstimator,
												IntFrequencyCounter frequencyCounter,
												IntUpdateableBuilder<IntRangeQuerySolver> histogram){
				this.cardinalityEstimator = cardinalityEstimator;
				this.frequencyCounter = frequencyCounter;
				this.minMaxCollector = new IntMinMaxCollector();
				this.histogram = histogram;
		}

		@Override public void update(Integer item) { update(item,1l); }
		@Override public void update(int item) { update(item,1l); }

		@Override
		public void update(int item, long count) {
				totalCount+=count;
				cardinalityEstimator.update(item,count);
				frequencyCounter.update(item,count);
		}

		@Override
		public void update(Integer item, long count) {
				if(item==null) {
						nullInts+=count;
						totalCount+=count;
						return;
				}
				update(item.intValue(),count);
		}

		@Override public void done() { hist = histogram.build(); }

		@Override
		public IntRangeQuerySolver querySolver() {
				if(hist==null) return histogram.build();
				return hist;
		}

		@Override
		public Set<? extends FrequencyEstimate<Integer>> mostFrequentElements(int maxFrequentElements){
				return frequencyCounter.getMostFrequentElements(maxFrequentElements);
		}

		@Override
		public Set<? extends FrequencyEstimate<Integer>> heavyHitters(float support){
				return frequencyCounter.getFrequentElements(support);
		}

		@Override public long cardinality(){ return cardinalityEstimator.getEstimate(); }
		@Override public long nullCount(){ return nullInts; }
		@Override public long nonNullCount() { return totalCount-nullInts; }
		@Override public Integer minValue() { return min(); }
		@Override public Integer maxValue() { return max(); }
		@Override public int max(){ return minMaxCollector.max(); }
		@Override public int min(){ return minMaxCollector.min(); }
}
