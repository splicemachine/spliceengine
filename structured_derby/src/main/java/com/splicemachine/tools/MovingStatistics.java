package com.splicemachine.tools;

/**
 * @author Scott Fines
 *         Date: 11/25/13
 */
public interface MovingStatistics {
		public double estimateMean();

		public double estimateVariance();

		public long numberOfMeasurements();

		public void update(double measurement);
}
