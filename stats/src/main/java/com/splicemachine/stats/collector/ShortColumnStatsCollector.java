package com.splicemachine.stats.collector;

import com.splicemachine.stats.ShortColumnStatistics;
import com.splicemachine.stats.ShortUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ShortColumnStatsCollector extends ColumnStatsCollector<Short>,ShortUpdateable{

    ShortColumnStatistics build();
}
