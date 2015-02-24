package com.splicemachine.stats.collector;

import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.LongUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface LongColumnStatsCollector extends ColumnStatsCollector<Long>,LongUpdateable {

    LongColumnStatistics build();

}
