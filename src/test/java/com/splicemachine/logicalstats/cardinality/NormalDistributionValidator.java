package com.splicemachine.logicalstats.cardinality;

import com.splicemachine.logicalstats.DoubleUpdateable;
import org.junit.Assert;

/**
 * Validates that a collection of estimates is normally distributed.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public class NormalDistributionValidator implements DoubleUpdateable{
		private long oneSigmas =0l;
		private long twoSigmas =0l;
		private long threeSigmas =0l;
		private long outliers =0l;

		private long numTrials = 0l;

		private final double sigma;
		private final double twoSigma;
		private final double threeSigma;


		public NormalDistributionValidator(double sigma) {
				this.sigma = sigma;
				this.twoSigma = 2*sigma;
				this.threeSigma = 3*sigma;
		}

		@Override
		public void update(double item) {
				numTrials++;
				if(item<=sigma)
						oneSigmas++;
				else if(item<=2*sigma)
						twoSigmas++;
				else if(item<=3*sigma)
						threeSigmas++;
				else
						outliers++;
		}

		@Override
		public void update(double item, long count) {

		}

		public long getOneSigmas() { return oneSigmas; }
		public long getTwoSigmas() { return twoSigmas; }
		public long getThreeSigmas() { return threeSigmas; }
		public long getOutliers() { return outliers; }

		/**
		 *
		 * @param tolerance a number between 0 and 1 which indicates how far away from correct the estimate
		 *                  can be before failing
		 */
		void assertIsNormallyDistributed(double tolerance){
				String sigmaString = String.format(" [total:%d, 3s:%d, 2s:%d, 1s:%d, outliers:%d]",numTrials,threeSigmas,twoSigmas,oneSigmas,outliers);
				//assert ordering--oneSigmas should be more than two sigmas, more than three Sigmas, more than outliers
				double percentage = ((double)oneSigmas)/numTrials;
				double requiredPercentage = 0.65-tolerance;
				Assert.assertTrue("Not enough 1-sigma events! percentage="+percentage+sigmaString,percentage>=requiredPercentage);
				percentage = ((double)oneSigmas+twoSigmas)/numTrials;
				requiredPercentage = 0.95-tolerance;
				Assert.assertTrue("Too few 2-sigma events! percentage="+percentage+sigmaString,percentage>=requiredPercentage);
				percentage = ((double)threeSigmas+oneSigmas+twoSigmas)/numTrials;
				requiredPercentage = 0.99-tolerance;
				Assert.assertTrue("Too few 3-sigma events! percentage="+percentage+sigmaString,percentage>=requiredPercentage);

		}

		@Override
		public void update(Double item) {

		}

		@Override
		public void update(Double item, long count) {

		}
}
