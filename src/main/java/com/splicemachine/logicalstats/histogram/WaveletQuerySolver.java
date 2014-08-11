package com.splicemachine.logicalstats.histogram;

/**
 * A RangeQuerySolver
 * @author Scott Fines
 * Date: 4/30/14
 */
public class WaveletQuerySolver<T extends Comparable<T>> implements RangeQuerySolver<T> {
		private int[] coefficientIndices;
		private long[] coefficients;

		public WaveletQuerySolver(int[] coefficientIndices, long[] coefficients) {
				this.coefficientIndices = coefficientIndices;
				this.coefficients = coefficients;
		}

		@Override
		public long getNumElements(T start, T end, boolean inclusiveStart, boolean inclusiveEnd) {
				return 0;
		}

		@Override
		public T getMin() {
				return null;
		}

		@Override
		public T getMax() {
				return null;
		}
}
