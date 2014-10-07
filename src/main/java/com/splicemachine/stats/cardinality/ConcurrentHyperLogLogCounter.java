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
