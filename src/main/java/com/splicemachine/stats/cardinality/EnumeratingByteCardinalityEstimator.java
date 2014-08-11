package com.splicemachine.stats.cardinality;

/**
 * Simple cardinality estimator which enumerates all the possible values for a single byte, and determines
 * whether or not the value is present.
 *
 * This class uses 256 booleans for storage, so it is highly efficient.
 *
 * @author Scott Fines
 * Date: 6/5/14
 */
public class EnumeratingByteCardinalityEstimator implements ByteCardinalityEstimator {
		private boolean[] present = new boolean[256];

		@Override
		public void update(byte item) {
				int pos = item & 0xff;
				present[pos] = true;
		}

		@Override
		public long getEstimate() {
				int distinct = 0;
				for (boolean isPresent : present) {
						if (isPresent)
								distinct++;
				}
				return distinct;
		}

		@Override
		public void update(byte item, long count) {
			update(item); //don't care about counts for cardinality estimates
		}

		@Override
		public void update(Byte item) {
				assert item!=null: "Cannot estimate null bytes!";
				update(item.byteValue());
		}

		@Override
		public void update(Byte item, long count) {
				assert item!=null: "Cannot estimate null bytes!";
				update(item.byteValue(),count);
		}
}
