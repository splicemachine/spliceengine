package com.splicemachine.logicalstats.cardinality;

import com.splicemachine.logicalstats.DoubleFunction;
import com.splicemachine.utils.hash.Hash64;

/**
 * An improved version of HyperLogLogCounter.
 *
 * <p>This implementation makes use of engineering observations made by Heule
 *  et al. in <a href="http://research.google.com/pubs/pub40671.html"/>, particularly in
 *  the area of small-cardinality estimation.</p>
 *
 * <p>The main advancement here is bias-adjustment for low cardinalities.</p>
 *
 * <p>HyperLogLog shows a strong bias towards over-estimating low cardinalities, which
 * dissappears as the cardinality increases. The original HyperLogLog paper (Flajot et. all in
 * <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf" />) partially corrects this
 * by empirically determining that the linear-counting algorithm
 * (<a href="http://dblab.kaist.ac.kr/Publication/pdf/ACM90_TODS_v15n2.pdf" />) shows better accuracy
 * at those cardinalities, so for cardinalities below 5m/2, it prefers LinearCounting to HyperLogLog.</p>
 *
 * <p>However, this creates a small pop in bias for cardinalities which are higher than a threshold,but
 * still small. To correct for this, Heule et all introduced an empirical biasAdjustment, which
 * uses an interpolation strategy to estimate the bias in small cardinality estimations, and adjusting
 * if necessary. </p>
 *
 * <p>This implementation makes use of a user-defined Interpolation function to correct
 * for small-cardinality errors as efficiently as possible. When no function is provided,
 * the default behavior is to use a simple linear interpolation against empirically
 * observed data from Heule et al. to estimate the bias. In general, this default behavior
 * is likely to be the best bet, unless the precision required is higher than 16 (the limit on available
 * empirical data).</p>
 *
 * <p>When evaluating data with high cardinality (>5*2^p), this implementation defaults
 * to the raw HyperLogLogCounter, and will emit results similarly to {@link com.splicemachine.logicalstats.cardinality.HyperLogLogCounter}.
 * Below that threshold, however, bias adjustments will come in to play, in an attempt to reduce the bias inherent in
 * the HyperLogLog algorithm.</p>
 *
 * @see com.splicemachine.logicalstats.cardinality.HyperLogLogCounter
 * @see com.splicemachine.logicalstats.cardinality.BaseLogLogCounter
 *
 * @author Scott Fines
 * Date: 12/30/13
 */
public abstract class BaseBiasAdjustedHyperLogLogCounter extends BaseHyperLogLogCounter{
		private final DoubleFunction biasAdjuster;
		private final double adjustmentThreshold;

		/*
		 * Empirically determined thresholds at which to switch between using the interpolated bias-adjusted answer,
		 * and linear-counting. Helps reduce the bias in small-cardinality estimations
		 */
		private static final double[] biasThresholds = new double[]{10,20,40,80,220,400,900,1800,3100,6500,11500,20000,50000};

		public BaseBiasAdjustedHyperLogLogCounter(int size, Hash64 hashFunction) {
				this(size,hashFunction, HyperLogLogBiasEstimators.biasEstimate(size));
		}

		public BaseBiasAdjustedHyperLogLogCounter(int precision,
																							Hash64 hashFunction,
																							DoubleFunction biasAdjuster) {
				super(precision, hashFunction);
				this.biasAdjuster = biasAdjuster;
				this.adjustmentThreshold = biasThresholds[precision-4];
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
				if(E <=5* numRegisters){
						E = E - biasAdjuster.y(E); //adjust for low-cardinality bias
						if(zeroRegisterCount!=0){
								//perform linear counting
								double h = numRegisters *Math.log((double) numRegisters /zeroRegisterCount);
								if(h <= adjustmentThreshold){
										//we are below the bias threshold, use linear counting instead
										E = h;
								}
						}
				}
				return (long)E;
		}
}
