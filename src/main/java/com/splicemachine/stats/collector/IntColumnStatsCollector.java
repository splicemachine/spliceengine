package com.splicemachine.stats.collector;

import com.splicemachine.stats.IntColumnStatistics;
import com.splicemachine.stats.IntUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface IntColumnStatsCollector extends ColumnStatsCollector<Integer>,IntUpdateable{

    IntColumnStatistics build();

    void updateNull();

    void updateNull(long count);

    void updateSize(int size);
}
