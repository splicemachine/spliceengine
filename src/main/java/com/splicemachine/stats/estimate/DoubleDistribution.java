package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface DoubleDistribution extends Distribution<Double> {

    long selectivity(double value);

    long rangeSelectivity(double start, double stop, boolean includeStart, boolean includeStop);
}
