package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.Hash64;

import java.nio.ByteBuffer;

/**
 * Implementation of a Raw LogLog Cardinality estimator.
 *
 * <p>This is an implementation of the original LogLogCounter algorithm,
 * as described by Durand and Flajolet in
 * "Loglog Counting of Large Cardinalities" (Durand, M. and Flajolet, P)
 * <a href="http://algo.inria.fr/flajolet/Publications/DuFl03-LNCS.pdf"/>,
 * with 64-bit instead of 32-bit hash functions.</p>
 *
 * <h3>Algorithm structure</h3>
 * <p>The essential structure is two-fold: a consistent, streaming <i>update</i>, with
 * a periodic <i>estimate</i></p>
 *
 * <p>When an update is requested, the entry is first hashed using a 64-bit hash function. The first
 * {@code precision} bits are used to decide a "bucket" to which this entry belongs, and the leftmost 1-bit
 * of the remaining {@code 64-precision} bits is used as a "value". If the entry in the bucket is less than
 * the value, then the value is placed in that bucket instead.
 *
 * <p>When an estimate is needed, then one merely computes the arithmetic mean over all buckets, then takes
 * {@code alphaM*2^mean} as the estimate of the cardinality, where {@code alphaM} is a pre-determined, fixed
 * constant dependent on the desired precision.</p>
 *
 * <h3>Accuracy</h3>
 * <p>This algorithm has probabilistic accuracy guarantees.In particular, the error is normally distributed, with
 * a mean error of 0 and a standard deviation of {@code S = 1.30/sqrt(2^precision)}. Thus, 65% of all estimates will
 * fall within {@code 1S}, 95% will fall within {@code 2S}, and 99% within {@code 3S}.</p>
 *
 * <p>As a side effect of that, increasing the precision will decrease the normal error. For example, a precision
 * of 4 will have an error threshold of {@code 1.30/sqrt(8) = 0.45}, while a precision of 8 will have an error of
 * {@code 1.30/sqrt(256) = 0.08}.</p>
 *
 * <p>Note that, if the precision is less than 3, the error does not remain bounded as cardinality increases.
 * As a result, this implementation will not allow a precision of less than 3.</p>
 *
 * <h3>Memory footprint</h3>
 * <p>The full memory footprint depends on the implementation of the storage buckets, but the algorithm requires
 * at least {@code 2^precision} buckets (referred to by the variable {@code numRegisters}, so increasing the precision
 * will also increase the required memory.</p>
 *
 * <p>Additionally, there are a few variables which are cached to avoid recomputation all the time. These fields
 * add a negligible memory overhead.</p>
 *
 * <h3>Performance</h3>
 * <p>Update performance is affected by two things: the cost to hash an element, and the cost to update a bucket.
 * In most implementations, the performance is dominated by the hash function chosen
 * (although in a highly concurrent environment,it is possible that the cost to update a
 * bucket may be significant).</p>
 *
 * @author Scott Fines
 * Date: 11/23/13
 */
public abstract class BaseLogLogCounter{
		protected final int numRegisters;
		protected final int precision;
		protected final Hash64 hashFunction;

		//cached for performance
		protected final double alphaM;
		protected final int shift;

		public BaseLogLogCounter(int precision, Hash64 hashFunction) {
				assert precision>=4;

				this.hashFunction = hashFunction;
				this.precision = precision;
				this.numRegisters = 1<< this.precision;
				this.shift = Long.SIZE- this.precision;

				alphaM = computeAlpha(this.precision, numRegisters);
		}

		public final void update(byte[] bytes, int offset, int length) {
				doUpdate(hashFunction.hash(bytes, offset, length));
		}

		public final void update(ByteBuffer bytes) {
				doUpdate(hashFunction.hash(bytes));
		}

		public void update(int item) {
				doUpdate(hashFunction.hash(item));
		}

		@SuppressWarnings("UnusedParameters")
		public void update(int item, long count) {
				update(item); //count doesn't matter for Cardinality checking
		}

		public void update(short item, long count){
				doUpdate(hashFunction.hash(item));
		}

		public final void update(long item) {
			doUpdate(hashFunction.hash(item));
		}

		public final void update(float item){
				doUpdate(hashFunction.hash(item));
		}

		public final void update(double item){
				doUpdate(hashFunction.hash(item));
		}

		public long getEstimate() {
				int total = 0;
				for(int i=0;i< numRegisters;i++){
						int val = getRegister(i);
						total+=val;
				}

				double z = Math.pow(2,total/((double) numRegisters));

				double E = alphaM*z;
				return (long)E;
		}

    /**
     * Merge this counter with another BaseLogLog counter.
     *
     * @param otherCounter the other counter to merge.
     */
    public void merge(BaseLogLogCounter otherCounter){
        assert otherCounter.numRegisters <= numRegisters: "Cannot merge counters unless the incoming has fewer registers than we do";
        for(int i=0;i<otherCounter.numRegisters;i++){
            updateRegister(i,otherCounter.getRegister(i));
        }
    }

    /**
     * Clone this counter
     * @return a new Counter with the same contents
     */
    public abstract BaseLogLogCounter getClone();

    /**
		 * Compute the multiplicative scale factor {@code alpha}. Generally,
		 * this is only overriden by optimized variations on the core algorithm
		 * (such as SuperLogLog or HyperLogLog).
		 *
		 * @param precision the precision to use
		 * @param numRegisters the maximum number of registers
		 * @return the multiplicative scale factor {@code alpha}.
		 */
		protected double computeAlpha(int precision,int numRegisters){
				switch(precision){
						case 4: return 20.1355*numRegisters;
						case 5: return 7.8365*numRegisters;
						default: return 0.39701*numRegisters;
				}
		}

		/**
		 * Update the specified register with the specified value.
		 *
		 * <p>As each register holds the maximum seen value, implementations should
		 * only update the register if the value already present is less than that passed in.</p>
		 *
		 * @param register the register to update
		 * @param value the value to set (if larger than what is currently present)
		 */
		protected abstract void updateRegister(int register, int value);

		/**
		 * Get the value currently held in this register.
		 *
		 * @param register the register of interest
		 * @return the current value in the register
		 */
		protected abstract int getRegister(int register);

/********************************************************************/
		/*private helper functions*/

		/**
		 * Performs the meat of the update operation.
		 *
		 * First, find the register by taking the first precision bits of the
		 * hash. Then, find the most significant (left-most) 1-bit in
		 * the remaining (64-precision) bits. If that value is more than the value
		 * which is currently contained in the register, then swap. Otherwise,
		 * ignore.
		 *
		 * <p>This is allowed to be overridden (for example, allowing sparse storage),
		 * but care should be taken to preserve the overall correctness
		 * of the algorithm, In general, it is not necessary to override this method,
		 * except in very specific circumstances</p>
		 *
		 * @param hash the hash of the entry
		 */
		protected void doUpdate(long hash) {
				int register = (int)(hash>>>shift);
				long w = hash << precision;
				int p = w==0l?1: (Long.numberOfLeadingZeros(w)+1);

				updateRegister(register, p);
		}

}
