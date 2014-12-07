package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface FloatFrequentElements extends FrequentElements<Float> {

    FloatFrequencyEstimate countEqual(float item);

    Set<FloatFrequencyEstimate> frequentBetween(float start, float stop, boolean includeStart, boolean includeStop);

    Set<FloatFrequencyEstimate> frequentAfter(float start, boolean includeStart);

    Set<FloatFrequencyEstimate> frequentBefore(float stop, boolean includeStop);
}
