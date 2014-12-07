package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface IntFrequentElements extends FrequentElements<Integer>{

    IntFrequencyEstimate countEqual(int item);

    Set<IntFrequencyEstimate> frequentBetween(int start, int stop, boolean includeMid,boolean includeMax);

    Set<IntFrequencyEstimate> frequentAfter(int start, boolean includeStart);

    Set<IntFrequencyEstimate> frequentBefore(int stop, boolean includeStop);

}
