package com.splicemachine.stats.collector;

import com.splicemachine.stats.IntUpdateable;
import com.splicemachine.stats.cardinality.IntCardinalityEstimator;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.IntFrequencyCounter;
import com.splicemachine.stats.order.IntMinMaxCollector;

import java.util.Set;

/**
 * Collector which accumulates Statistical information for a specific entry.
 *
 * @author Scott Fines
 * Date: 6/5/14
 */
public class IntStatsCollector implements IntUpdateable{
		private long totalCount = 0l;
		private long nullInts = 0l;

		private final IntCardinalityEstimator cardinalityEstimator;
		private final IntMinMaxCollector minMaxCollector;
		private final IntFrequencyCounter frequencyCounter;

		//TODO -sf- add Wavelet histogram

		public IntStatsCollector(IntCardinalityEstimator cardinalityEstimator,
														 IntFrequencyCounter frequencyCounter) {
				this.minMaxCollector = new IntMinMaxCollector();
				this.cardinalityEstimator = cardinalityEstimator;
				this.frequencyCounter = frequencyCounter;
		}

		@Override public void update(int item) { update(item,1l); }

		@Override
		public void update(int item, long count) {
				totalCount+=count;
				cardinalityEstimator.update(item,count);
				frequencyCounter.update(item,count);
				minMaxCollector.update(item);
		}

		@Override public void update(Integer item) { update(item,1l); }

		@Override
		public void update(Integer item, long count) {
				if(item==null) {
						nullInts+=count;
						return;
				}
				update(item.intValue(),count);
		}

		public long cardinality(){ return cardinalityEstimator.getEstimate(); }

		public Set<? extends FrequencyEstimate<Integer>> getMostFrequentElements(int maxFrequentElements){
				return frequencyCounter.getMostFrequentElements(maxFrequentElements);
		}

		public Set<? extends FrequencyEstimate<Integer>> getHeavyHitters(float support){
				return frequencyCounter.getFrequentElements(support);
		}

		public long count(){ return totalCount; }
		public long nullCount(){ return nullInts; }
		public int max(){ return minMaxCollector.max(); }
		public int min(){ return minMaxCollector.min(); }

}
