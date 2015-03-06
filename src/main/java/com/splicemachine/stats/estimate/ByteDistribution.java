package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface ByteDistribution extends Distribution<Byte>{

    long selectivity(byte value);

    long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop);
}
