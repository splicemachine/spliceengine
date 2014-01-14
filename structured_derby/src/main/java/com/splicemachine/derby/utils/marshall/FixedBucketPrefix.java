package com.splicemachine.derby.utils.marshall;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class FixedBucketPrefix implements HashPrefix{
		private final byte bucket;
		private final HashPrefix delegate;

		public FixedBucketPrefix(byte bucket, HashPrefix delegate) {
				this.bucket = bucket;
				this.delegate = delegate;
		}

		@Override
		public int getPrefixLength() {
				return delegate.getPrefixLength() + 1;
		}

		@Override
		public void encode(byte[] bytes, int offset, byte[] hashBytes) {
				bytes[offset] = bucket;
				delegate.encode(bytes, offset+1, hashBytes);
		}
}
