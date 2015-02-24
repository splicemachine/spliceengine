package com.splicemachine.stats.frequency;

import java.util.Arrays;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
public class EnumeratingByteFrequencyCounter implements ByteFrequencyCounter {
		private final long[] counts = new long[256];

		@Override public void update(byte item) { update(item,1l); }
		@Override public void update(Byte item) { update(item,1l); }

		@Override
		public void update(Byte item, long count) {
				assert item!=null : "Cannot add null elements!";
				update(item.byteValue(),count);
		}

		@Override
		public void update(byte item, long count) {
				counts[(item & 0xff)]+=count;
		}

    @Override
		public ByteFrequentElements heavyHitters(float support) {
				return new ByteHeavyHitters(Arrays.copyOf(counts,256),support);
		}

    @Override
		public ByteFrequentElements frequentElements(int k) {
				return new ByteFrequencies(counts,k);
		}

}
