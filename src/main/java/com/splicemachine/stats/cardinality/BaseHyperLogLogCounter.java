package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.Hash64;

/**
 * Base implementation of the HyperLogLog Cardinality estimator.
 *
 * <p>This is a slightly-modified implementation of HyperLogLog, as constructed by Flajolet et. al
 * in <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf" />. In particular,
 * this implementation uses 64-bit hash functions, which eliminates the need for large-range
 * error corrections near {@code 2^32}</p>
 *
 * <p>The update approach is identical to that of the base Loglog counter, but the estimation
 * procedure is different.</p>
 *
 * <h3>Error Estimate</h3>
 *
 * <p>HyperLogLog has a base error which is distributed normally, with a standard deviation
 * of {@code 1.04/sqrt(number of registers)}. This means that 65% of the time, the error will
 * be below that, 95% of the time will be below twice that error, and 99% of the time will
 * have error less than {@code 3.12/sqrt(number of registers)}.</p>
 *
 * <p>However, the error present in HyperLogLog grows substantially for very low observed
 * cardinalities. To correct for this, we shift to using a LinearCounting algorithm when the
 * estimated cardinality is below an empirically determined threshold. This mostly corrects
 * for the error, but is an inferior correction than that presented with
 * {@link com.splicemachine.stats.cardinality.AdjustedHyperLogLogCounter}, and thus this implementation
 * should be favored only when the expected cardinality is very high.</p>
 *
 * @see com.splicemachine.stats.cardinality.AdjustedHyperLogLogCounter
 * @see com.splicemachine.stats.cardinality.BaseLogLogCounter
 *
 * @author Scott Fines
 * Date: 12/30/13
 */
public abstract class BaseHyperLogLogCounter extends BaseLogLogCounter{

		public BaseHyperLogLogCounter(int precision, Hash64 hashFunction) {
				super(precision, hashFunction);
		}

		@Override
		protected double computeAlpha(int b, int numRegisters) {
				switch(numRegisters){
						case 16: return 0.673d;
						case 32: return 0.697d;
						case 64: return 0.709d;
						default:
								return 0.7213/(1+1.079d/ numRegisters);
				}
		}

		@Override
		public long getEstimate() {
				double z = 0d;
				int zeroRegisterCount = 0;
				for(int i=0;i< numRegisters;i++){
						int value = getRegister(i);
						if(value==0)
								zeroRegisterCount++;
						z+=1d/(1<<value);
				}

				double E = alphaM*Math.pow(numRegisters,2)/z;
				if(E <= 5*numRegisters/2){
						E = numRegisters*Math.log((double)numRegisters/zeroRegisterCount);
				}
				return (long)E;
		}
}
