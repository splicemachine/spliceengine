package com.splicemachine.stats.collector;

import com.splicemachine.stats.IntUpdateable;
import com.splicemachine.stats.histogram.IntRangeQuerySolver;

/**
 * Integer-specific interface to allow users to avoid auto-boxing integers
 * where needed.
 *
 * @author Scott Fines
 *         Date: 12/1/14
 */
public interface IntStatsCollector extends IntUpdateable,StatsCollector<Integer>{

    int min();

    int max();

    IntRangeQuerySolver querySolver();
}
