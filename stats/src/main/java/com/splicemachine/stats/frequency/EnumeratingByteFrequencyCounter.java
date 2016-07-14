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

package com.splicemachine.stats.frequency;

import java.util.Arrays;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class EnumeratingByteFrequencyCounter implements ByteFrequencyCounter {
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
