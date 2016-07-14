/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils.marshall;

import java.io.IOException;

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

		@Override public void close() throws IOException {  }
}
