package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface DoubleStatistics extends ColumnStatistics<Double> {

    double min();

    double max();
}
