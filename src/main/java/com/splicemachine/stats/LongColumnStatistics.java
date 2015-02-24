package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface LongColumnStatistics extends ColumnStatistics<Long> {

    long min();

    long max();

}
