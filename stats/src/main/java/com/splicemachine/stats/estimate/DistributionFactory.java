package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface DistributionFactory<T extends Comparable<T>> {

    Distribution<T> newDistribution(ColumnStatistics<T> statistics);
}
