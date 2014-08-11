package com.splicemachine.logicalstats.histogram;

/**
 * @author Scott Fines
 *         Date: 4/13/14
 */
public interface HistogramBuilder<T extends Comparable<T>> {

		Histogram<T> build();
}
