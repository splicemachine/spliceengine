package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface LongFrequentElements extends FrequentElements<Long> {

    LongFrequencyEstimate countEqual(long item);

    Set<LongFrequencyEstimate> frequentBetween(long start, long stop, boolean includeStart, boolean includeStop);

    Set<LongFrequencyEstimate> frequentAfter(long start, boolean includeStart);

    Set<LongFrequencyEstimate> frequentBefore(long stop, boolean includeStop);
}
