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


import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.hash.Hash64;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Concurrent implementation of the HyperLogLog counter.
 *
 * <p>This class is thread-safe.</p>
 *
 * @author Scott Fines
 * Date: 12/30/13
 */
@ThreadSafe
public class ConcurrentHyperLogLogCounter extends BaseHyperLogLogCounter {
    private final AtomicIntegerArray buckets;

    public ConcurrentHyperLogLogCounter(int size, Hash64 hashFunction) {
        super(size, hashFunction);
        this.buckets = new AtomicIntegerArray(numRegisters);
    }

    private ConcurrentHyperLogLogCounter(int precision, Hash64 hashFunction, AtomicIntegerArray buckets) {
        super(precision, hashFunction);
        this.buckets = buckets;
    }

    @Override
    public BaseLogLogCounter getClone() {
        AtomicIntegerArray atomicIntegerArray = new AtomicIntegerArray(buckets.length());
        for(int i=0;i<buckets.length();i++){
            atomicIntegerArray.set(i,buckets.get(i));
        }
        return new ConcurrentHyperLogLogCounter(precision,hashFunction, atomicIntegerArray);
    }

    /**
     * Clear the current value of this counter. This is useful
     * when the counter is keeping track of real-life measurements.
     *
     * Note: this is <em>not</em> atomic--it is possible that, even
     * as it clears elements, more elements can be added in, resulting in
     * a weak clear--e.g some elements may not be counted, and some elements
     * may be considered twice. Thus, it is recommended that this method
     * be used only when approximate clearing is needed. Otherwise, synchronization
     * would be required (which we don't want).
     */
	public void clear(){
		for(int i=0;i<buckets.length();i++){
			buckets.set(i,0);
		}
	}

    @Override
    protected void updateRegister(int register, int value) {
        boolean success =false;
        while(!success){
            int currByte = buckets.get(register);
            if(currByte>=value) return;

            success = buckets.compareAndSet(register,currByte,value);
        }
    }

    @Override
    protected int getRegister(int register) {
        return buckets.get(register);
    }
}
