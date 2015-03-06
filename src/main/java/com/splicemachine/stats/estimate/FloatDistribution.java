package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface FloatDistribution extends Distribution<Float>  {

    long selectivity(float value);

    long selectivityBefore(float stop, boolean includeStop);

    long selectivityAfter(float start, boolean includeStart);

    long rangeSelectivity(float start, float stop, boolean includeStart, boolean includeStop);
}
