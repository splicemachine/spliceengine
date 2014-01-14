package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.impl.sql.execute.operations.RowKeyDistributorByHashPrefix;

/**
 * Implementation of a compact RowKeyDistributorByHashPrefix.Hasher
 *
 * @author Scott Fines
 * Date: 11/18/13
 */
public class BucketHasher implements RowKeyDistributorByHashPrefix.Hasher {
		private static final BucketHasher[] INSTANCES;

		static{
				SpreadBucket[] values = SpreadBucket.values();
				INSTANCES = new BucketHasher[values.length];
				for(SpreadBucket spreadBucket: values){
						INSTANCES[spreadBucket.ordinal()] = new BucketHasher(spreadBucket);
				}
		}
		private final byte[][] buckets;

		private BucketHasher(SpreadBucket spreadBucket) {
				byte mask = spreadBucket.getMask();
				int numBuckets = spreadBucket.getNumBuckets();
				buckets = new byte[numBuckets][];
				for(int i=0;i< numBuckets;i++){
						buckets[i] = new byte[]{(byte)(i*mask)};
				}
		}

		public static BucketHasher getHasher(SpreadBucket bucket){
				return INSTANCES[bucket.ordinal()];
		}

		@Override
		public byte[] getHashPrefix(byte[] originalKey) {
				throw new UnsupportedOperationException();
		}

		@Override
		public byte[][] getAllPossiblePrefixes() {
				return buckets;
		}

		@Override
		public int getPrefixLength(byte[] adjustedKey) {
				return 1;
		}
}
