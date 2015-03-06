package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface DoubleDistribution extends Distribution<Double> {

    long selectivity(double value);

    long selectivityBefore(double stop, boolean includeStop);

    long selectivityAfter(double start, boolean includeStart);

    long rangeSelectivity(double start, double stop, boolean includeStart, boolean includeStop);
}
