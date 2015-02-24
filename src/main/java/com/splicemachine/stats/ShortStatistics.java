package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ShortStatistics extends ColumnStatistics<Short> {

    short min();

    short max();
}
