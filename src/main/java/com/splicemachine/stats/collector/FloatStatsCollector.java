package com.splicemachine.stats.collector;

import com.splicemachine.stats.FloatStatistics;
import com.splicemachine.stats.FloatUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface FloatStatsCollector extends ColumnStatsCollector<Float>,FloatUpdateable {

    FloatStatistics build();
}
