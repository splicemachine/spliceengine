package com.splicemachine.stats.cardinality;


import com.splicemachine.hash.Hash64;
import com.splicemachine.primitives.Bytes;

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

    @Override
    public byte[] encode() {
        byte[] data = new byte[8+buckets.length];
        int pos =0;
        Bytes.toBytes(precision,data,pos);
        pos+=4;
        Bytes.toBytes(buckets.length,data,pos);
        pos+=4;
        System.arraycopy(buckets,0,data,pos,buckets.length);
        return data;
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
