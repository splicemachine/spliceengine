package com.splicemachine.stats.collector;

import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.FloatUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface FloatColumnStatsCollector extends ColumnStatsCollector<Float>,FloatUpdateable {

    FloatColumnStatistics build();
}
