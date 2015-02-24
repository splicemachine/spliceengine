package com.splicemachine.stats.collector;

import com.splicemachine.stats.BooleanStatistics;
import com.splicemachine.stats.BooleanUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface BooleanStatsCollector extends ColumnStatsCollector<Boolean>,BooleanUpdateable{

    BooleanStatistics build();
}
