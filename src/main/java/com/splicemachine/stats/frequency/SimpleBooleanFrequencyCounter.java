package com.splicemachine.stats.frequency;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
public class SimpleBooleanFrequencyCounter implements BooleanFrequencyCounter {
		private Frequency trueFrequency = new Frequency(true);
		private Frequency falseFrequency = new Frequency(false);

		@Override
		public void update(boolean item) {
				if(item) trueFrequency.count++;
				else falseFrequency.count++;
		}

		@Override
		public void update(Boolean item) {
				if(item==Boolean.TRUE) trueFrequency.count++;
				else falseFrequency.count++;
		}

		@Override
		public void update(Boolean item, long count) {
				if(item==Boolean.TRUE) trueFrequency.count+=count;
				else falseFrequency.count+=count;
		}

		@Override
		public Set<FrequencyEstimate<Boolean>> getFrequentElements(float support) {
				final long threshold = (long)Math.ceil(support * (trueFrequency.count+falseFrequency.count));

				final List<FrequencyEstimate<Boolean>> estimates = Lists.newArrayListWithCapacity(2);
				if(trueFrequency.count>threshold)
						estimates.add(trueFrequency);
				if(falseFrequency.count>threshold)
						estimates.add(falseFrequency);

				return new AbstractSet<FrequencyEstimate<Boolean>>() {
						@Override
						public Iterator<FrequencyEstimate<Boolean>> iterator() {
								return estimates.iterator();
						}

						@Override
						public int size() {
								return estimates.size();
						}
				};
		}

		@Override
		public void update(boolean item, long count) {
				if(item) trueFrequency.count+=count;
				else falseFrequency.count+=count;
		}

		@Override
		public BooleanFrequentElements frequencies() {
				return new SimpleBooleanFrequentElements(trueFrequency.count,falseFrequency.count);
		}

		@Override
		public Set<FrequencyEstimate<Boolean>> getMostFrequentElements(int k) {
				if(k==0) return Collections.emptySet();
				if(k==1){
						final Frequency toUse = trueFrequency.count > falseFrequency.count? trueFrequency:falseFrequency;
						return new AbstractSet<FrequencyEstimate<Boolean>>() {
								@Override
								public Iterator<FrequencyEstimate<Boolean>> iterator() {
										return Iterators.<FrequencyEstimate<Boolean>>singletonIterator(toUse);
								}
								@Override public int size() { return 1; }
						};
				}else{
						return new AbstractSet<FrequencyEstimate<Boolean>>() {
								@Override
								public Iterator<FrequencyEstimate<Boolean>> iterator() {
										if(trueFrequency.count>falseFrequency.count)
												return Iterators.<FrequencyEstimate<Boolean>>forArray(trueFrequency,falseFrequency);
										else
												return Iterators.<FrequencyEstimate<Boolean>>forArray(falseFrequency,trueFrequency);
								}

								@Override public int size() { return 2; }
						};
				}
		}

		@Override
		public Iterator<FrequencyEstimate<Boolean>> iterator() {
				return Iterators.<FrequencyEstimate<Boolean>>forArray(trueFrequency,falseFrequency);
		}

		private static class Frequency implements BooleanFrequencyEstimate{
				private long count;
				private final boolean value;

				private Frequency(boolean value) {
						this.value = value;
				}

				@Override public boolean value() { return value; }
				@Override public Boolean getValue() { return value; }
				@Override public long count() { return count; }
				@Override public long error() { return 0; } //no error! whoo!
		}

}
