package com.splicemachine.stats.frequency;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import java.util.*;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
public class EnumeratingByteFrequencyCounter implements ByteFrequencyCounter {
		private final long[] counts = new long[256];
		private long total = 0l;

		@Override public void update(byte item) { update(item,1l); }
		@Override public void update(Byte item) { update(item,1l); }

		@Override
		public void update(Byte item, long count) {
				assert item!=null : "Cannot add null elements!";
				update(item.byteValue(),count);
		}

		@Override
		public void update(byte item, long count) {
				total++;
				counts[(item & 0xff)]+=count;
		}

		@Override
		public Set<? extends FrequencyEstimate<Byte>> getFrequentElements(float support) {
				long threshold = (long)Math.ceil(support*total);
				Set<Freq> data = Sets.newTreeSet();

				for(int i=0;i<counts.length;i++){
						if(counts[i]>=threshold){
							data.add(new Freq((byte)i,counts[i]));
						}
				}
				return data;
		}

		@Override
		public ByteFrequentElements heavyHitters(float support) {
				long threshold = (long)Math.ceil(support*total);

				return new ByteHeavyHitters(Arrays.copyOf(counts,256),threshold);
		}

		@Override
		public Set<? extends FrequencyEstimate<Byte>> getMostFrequentElements(int k) {
				List<Freq> freqs = Lists.newArrayListWithCapacity(counts.length);
				for(int i=0;i<counts.length;i++){
						freqs.add(new Freq((byte)i,counts[i]));
				}
				Collections.sort(freqs);
				return Sets.newTreeSet(freqs.subList(0,k));
		}

		@Override
		public ByteFrequentElements frequentElements(int k) {
				return new ByteFrequencies(counts,k);
		}

		@Override public Iterator<FrequencyEstimate<Byte>> iterator() { return new Iter(); }

		private static class Freq implements ByteFrequencyEstimate{
				private byte value;
				private long count;

				public Freq(byte value, long count) {
						this.value = value;
						this.count = count;
				}

				@Override public Byte getValue() { return value; }
				@Override public long count() { return count; }
				@Override public long error() { return 0; }
				@Override public byte value() { return value; }

				@Override
				@SuppressWarnings("NullableProblems")
				public int compareTo(ByteFrequencyEstimate o) {
						int compare = Longs.compare(count, o.count());
						if(compare!=0) return -1*compare;
						return value - o.value();
				}

		}

    private class Iter implements Iterator<FrequencyEstimate<Byte>> {
        private int position = 0;

        @Override public boolean hasNext() { return position< counts.length; }
        @Override public void remove() { throw new UnsupportedOperationException("Cannot remove entries"); }

        @Override
        public FrequencyEstimate<Byte> next() {
            if(!hasNext()) throw new NoSuchElementException();
            int pos = position;
            long count = counts[pos];
            position++;
            return new Freq((byte)pos,count);
        }

    }
}
