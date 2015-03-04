package com.splicemachine.stats.frequency;

import com.splicemachine.stats.Mergeable;

import java.util.Set;

/**
 * Representation of the <em>Frequent Elements</em> in a multiset.
 *
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface FrequentElements<T>  extends Mergeable<FrequentElements<T>> {

    FrequencyEstimate<? extends T> equal(T item);

    Set<? extends FrequencyEstimate<T>> frequentElementsBetween(T start, T stop, boolean includeMin, boolean includeStop);

    Set<? extends FrequencyEstimate<T>> allFrequentElements();

    FrequentElements<T> getClone();
}
