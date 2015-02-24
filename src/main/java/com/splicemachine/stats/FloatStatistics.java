package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface FloatStatistics extends ColumnStatistics<Float> {

    float min();

    float max();
}
