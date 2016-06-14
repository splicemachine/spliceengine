package com.splicemachine.stats.frequency;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface ByteFrequencyEstimate extends FrequencyEstimate<Byte>,Comparable<ByteFrequencyEstimate> {

    byte value();
}
