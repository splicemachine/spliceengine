package com.splicemachine.stats.collector;

import com.splicemachine.stats.BooleanColumnStatistics;
import com.splicemachine.stats.BooleanUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface BooleanStatsCollector extends ColumnStatsCollector<Boolean>,BooleanUpdateable{

    BooleanColumnStatistics build();
}
