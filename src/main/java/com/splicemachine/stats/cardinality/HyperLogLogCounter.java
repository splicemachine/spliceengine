package com.splicemachine.stats.cardinality;


import com.splicemachine.hash.Hash64;
import com.splicemachine.primitives.Bytes;

import java.util.Arrays;

/**
 * Basic implementation of the HyperLogLog Cardinality estimator.
 *
 * <p>This is an implementation of HyperLogLog, as constructed by Flajolet et. al
 * in <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf" /></p>
 *
 * This class is not thread safe. For a thread-safe implementation see
 * {@link com.splicemachine.stats.cardinality.ConcurrentHyperLogLogCounter}
 *
 * @author Scott Fines
 * Date: 12/30/13
 */
public class HyperLogLogCounter extends BaseHyperLogLogCounter{
		protected final byte[] buckets;

		public HyperLogLogCounter(int size, Hash64 hashFunction) {
				super(size, hashFunction);
				this.buckets = new byte[numRegisters];
		}

    @Override
    public BaseLogLogCounter getClone() {
        return new HyperLogLogCounter(super.precision, Arrays.copyOf(buckets,buckets.length),hashFunction);
    }

    public HyperLogLogCounter(int size,byte[] buckets, Hash64 hashFunction) {
        super(size, hashFunction);
        this.buckets = buckets;
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

    public static HyperLogLogCounter decode(Hash64 newHash, byte[] data, int offset){
        int pos = offset;
        int precision = Bytes.toInt(data, pos);
        pos+=4;
        int bucketSize = Bytes.toInt(data,pos);
        pos+=4;
        byte[] registers = new byte[bucketSize];
        System.arraycopy(data,pos,registers,0,bucketSize);
        return new HyperLogLogCounter(precision,registers,newHash);
    }
}
