package com.splicemachine.stats.frequency;

import com.splicemachine.stats.Mergeable;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface ByteFrequentElements extends FrequentElements<Byte>,Mergeable<ByteFrequentElements> {

    ByteFrequencyEstimate countEqual(byte item);

    Set<ByteFrequencyEstimate> frequentBetween(byte start, byte stop, boolean includeMid, boolean includeMax);

    Set<ByteFrequencyEstimate> frequentAfter(byte start, boolean includeStart);

    Set<ByteFrequencyEstimate> frequentBefore(byte stop, boolean includeStop);
}
