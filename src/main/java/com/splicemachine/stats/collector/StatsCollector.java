package com.splicemachine.stats.collector;

import com.splicemachine.stats.Updateable;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.histogram.RangeQuerySolver;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 10/24/14
 */
public interface StatsCollector<T extends Comparable<T>> extends Updateable<T> {

    FrequentElements<T> mostFrequentElements(int maxSize);

    FrequentElements<T> heavyHitters(float support);

    long cardinality();

    long nonNullCount();

    long nullCount();

    T minValue();

    T maxValue();

    RangeQuerySolver<T> querySolver();

    void done();
}
