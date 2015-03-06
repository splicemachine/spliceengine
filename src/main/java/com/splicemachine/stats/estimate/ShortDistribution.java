package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface ShortDistribution extends Distribution<Short>{

    long selectivity(short value);

    long selectivityBefore(short stop, boolean includeStop);

    long selectivityAfter(short start, boolean includeStart);

    long rangeSelectivity(short start, short stop, boolean includeStart, boolean includeStop);
}
