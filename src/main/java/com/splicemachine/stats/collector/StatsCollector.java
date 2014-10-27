package com.splicemachine.stats.collector;

import com.splicemachine.stats.Updateable;
import com.splicemachine.stats.frequency.FrequencyEstimate;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 10/24/14
 */
public interface StatsCollector<T extends Comparable<T>> extends Updateable<T> {

    Set<? extends FrequencyEstimate<T>> mostFrequentElements();

    long numDistinctElements();

    long numNonNullElements();

    long numNullElements();

    T min();

    T max();
}
