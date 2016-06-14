package com.splicemachine.stats.collector;

import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ByteUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ByteColumnStatsCollector extends ColumnStatsCollector<Byte>,ByteUpdateable {

    ByteColumnStatistics build();
}
