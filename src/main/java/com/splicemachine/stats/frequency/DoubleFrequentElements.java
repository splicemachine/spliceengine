package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface DoubleFrequentElements extends FrequentElements<Double>{

    DoubleFrequencyEstimate countEqual(double item);

    Set<DoubleFrequencyEstimate> frequentBetween(double start, double stop, boolean includeMid,boolean includeMax);

    Set<DoubleFrequencyEstimate> frequentAfter(double start, boolean includeStart);

    Set<DoubleFrequencyEstimate> frequentBefore(double stop, boolean includeStop);
}
