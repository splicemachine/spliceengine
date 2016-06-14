package com.splicemachine.stats.histogram;

/**
 * @author Scott Fines
 *         Date: 12/1/14
 */
public interface ByteRangeQuerySolver extends RangeQuerySolver<Byte>{

    byte max();

    byte min();

    long equal(byte value);

    long between(byte startValue, byte endValue, boolean inclusiveStart, boolean inclusiveEnd);

    long after(byte n, boolean equals);

    long before(byte n, boolean equals);
}
