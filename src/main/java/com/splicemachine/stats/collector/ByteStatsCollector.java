package com.splicemachine.stats.collector;

import com.splicemachine.stats.ByteStatistics;
import com.splicemachine.stats.ByteUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ByteStatsCollector extends ColumnStatsCollector<Byte>,ByteUpdateable {

    ByteStatistics build();
}
