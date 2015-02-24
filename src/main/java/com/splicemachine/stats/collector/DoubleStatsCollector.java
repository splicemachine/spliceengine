package com.splicemachine.stats.collector;

import com.splicemachine.stats.DoubleStatistics;
import com.splicemachine.stats.DoubleUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface DoubleStatsCollector extends ColumnStatsCollector<Double>,DoubleUpdateable {

    DoubleStatistics build();
}
