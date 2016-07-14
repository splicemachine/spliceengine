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

package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.Hash64;
import com.splicemachine.stats.DoubleFunction;

import java.util.Arrays;

/**
 * Cardinality Estimator with automatic bias-adjustment for low-cardinality estimations.
 *
 * <p>This is a simple, non-thread-safe implementation which uses a dense array for storage.
 * Thus the memory requirement is 1 byte per register.</p>
 *
 * @author Scott Fines
 * Date: 1/1/14
 */
public class AdjustedHyperLogLogCounter extends BaseBiasAdjustedHyperLogLogCounter {
		final byte[] buckets;


		public AdjustedHyperLogLogCounter(int size, Hash64 hashFunction) {
				super(size, hashFunction);
				this.buckets = new byte[numRegisters];
		}

    private AdjustedHyperLogLogCounter(int precision, Hash64 hashFunction, byte[] bytes) {
        super(precision, hashFunction);
        this.buckets = bytes;
    }

    @Override
    public BaseLogLogCounter getClone() {
        return new AdjustedHyperLogLogCounter(precision,hashFunction, Arrays.copyOf(buckets,buckets.length));
    }

    public AdjustedHyperLogLogCounter(int precision, Hash64 hashFunction, DoubleFunction biasAdjuster) {
				super(precision, hashFunction, biasAdjuster);
				this.buckets = new byte[numRegisters];
		}

		@Override
		protected void updateRegister(int register, int value) {
				byte b = buckets[register];
				if(b>=value) return;
				buckets[register] = (byte)(value & 0xff);
		}

		@Override
		protected int getRegister(int register) {
				return buckets[register];
		}

}
