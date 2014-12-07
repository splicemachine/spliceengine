package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface ShortFrequentElements extends FrequentElements<Short>{

    ShortFrequencyEstimate countEqual(short item);

    Set<ShortFrequencyEstimate> frequentBetween(short start, short stop, boolean includeMid,boolean includeMax);

    Set<ShortFrequencyEstimate> frequentBefore(short end, boolean includeStop);

    Set<ShortFrequencyEstimate> frequentAfter(short start, boolean includeStart);
}
