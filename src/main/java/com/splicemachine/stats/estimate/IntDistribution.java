package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface IntDistribution extends Distribution<Integer> {

    long selectivity(int value);

    long selectivityBefore(int stop, boolean includeStop);

    long selectivityAfter(int start, boolean includeStart);

    long rangeSelectivity(int start, int stop, boolean includeStart, boolean includeStop);
}
