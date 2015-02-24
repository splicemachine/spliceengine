package com.splicemachine.stats.collector;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.Updateable;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ColumnStatsCollector<E> extends Updateable<E>{

    ColumnStatistics<E> build();

    void updateNull();

    void updateNull(long count);

    void updateSize(int size);
}
