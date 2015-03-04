package com.splicemachine.stats.frequency;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface ByteFrequentElements extends FrequentElements<Byte> {

    ByteFrequencyEstimate countEqual(byte item);

    Set<ByteFrequencyEstimate> frequentBetween(byte start, byte stop, boolean includeMid, boolean includeMax);

    Set<ByteFrequencyEstimate> frequentAfter(byte start, boolean includeStart);

    Set<ByteFrequencyEstimate> frequentBefore(byte stop, boolean includeStop);

    ByteFrequentElements getClone();
}
