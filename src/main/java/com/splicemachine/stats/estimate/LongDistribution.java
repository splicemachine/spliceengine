package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface LongDistribution extends Distribution<Long>{

    long selectivity(long value);

    long rangeSelectivity(long start, long stop, boolean includeStart, boolean includeStop);
}
