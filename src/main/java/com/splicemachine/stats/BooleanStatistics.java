package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface BooleanStatistics extends ColumnStatistics<Boolean> {

    long trueCount();

    long falseCount();

}
