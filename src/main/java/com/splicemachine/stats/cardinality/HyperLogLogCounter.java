package com.splicemachine.stats.cardinality;

import com.splicemachine.utils.hash.Hash64;

/**
 * Basic implementation of the HyperLogLog Cardinality estimator.
 *
 * <p>This is an implementation of HyperLogLog, as constructed by Flajolet et. al
 * in <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf" /></p>
 *
 * This class is not thread safe. For a thread-safe implementation see
 * {@link org.streamstats.cardinality.ConcurrentHyperLogLogCounter}
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
