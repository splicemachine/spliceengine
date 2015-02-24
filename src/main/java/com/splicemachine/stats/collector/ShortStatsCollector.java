package com.splicemachine.stats.collector;

import com.splicemachine.stats.ShortStatistics;
import com.splicemachine.stats.ShortUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ShortStatsCollector extends ColumnStatsCollector<Short>,ShortUpdateable{

    ShortStatistics build();
}
