package com.splicemachine.derby.utils.marshall;


import com.splicemachine.utils.hash.ByteHash32;

/**
 * Prefix which appends a 1-byte "bucket" to the front
 * of the prefix.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public class BucketingPrefix implements HashPrefix{
		private final HashPrefix delegate;
		private final ByteHash32 hashFunction;
		private final SpreadBucket spreadBucket;

		public BucketingPrefix(HashPrefix delegate,
													 ByteHash32 hashFunction,
													 SpreadBucket spreadBucket) {
				this.delegate = delegate;
				this.hashFunction = hashFunction;
				this.spreadBucket = spreadBucket;
		}

		@Override
		public int getPrefixLength() {
				if(delegate!=null)
						return delegate.getPrefixLength()+1;
				return 1;
		}

		@Override
		public void encode(byte[] bytes, int offset, byte[] hashBytes) {
				bytes[offset] = spreadBucket.bucket(hashFunction.hash(hashBytes,0,hashBytes.length));
				if(delegate!=null)
						delegate.encode(bytes,offset+1,hashBytes);
		}

}
