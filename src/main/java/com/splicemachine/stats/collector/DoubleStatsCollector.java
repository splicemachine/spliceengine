package com.splicemachine.stats.collector;

import com.splicemachine.stats.DoubleColumnStatistics;
import com.splicemachine.stats.DoubleUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface DoubleStatsCollector extends ColumnStatsCollector<Double>,DoubleUpdateable {

    DoubleColumnStatistics build();
}
