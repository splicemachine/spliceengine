package com.splicemachine.stats.frequency;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface BytesFrequentElements extends FrequentElements<byte[]>{

    FrequencyEstimate<byte[]> countEqual(byte[] bytes, int offset, int length);

    FrequencyEstimate<byte[]> countEqual(ByteBuffer buffer);

    Set<IntFrequencyEstimate> frequentBetween(byte[] start, int startOffset,int startLength,
                                              byte[] stop, int stopOffset, int stopLength,
                                              boolean includeStart, boolean includeStop);

    Set<IntFrequencyEstimate> frequentBetween(ByteBuffer start, ByteBuffer stop,boolean includeStart, boolean includeStop);

    Set<IntFrequencyEstimate> frequentAfter(byte[] start, int startOffset,int startLength,
                                              boolean includeStart, boolean includeStop);

    Set<IntFrequencyEstimate> frequentAfter(ByteBuffer start, boolean includeStart);

    Set<IntFrequencyEstimate> frequentBefore(byte[] stop, int stopOffset, int stopLength,
                                              boolean includeStart, boolean includeStop);

    Set<IntFrequencyEstimate> frequentBefore(ByteBuffer stop, boolean includeStop);
}
