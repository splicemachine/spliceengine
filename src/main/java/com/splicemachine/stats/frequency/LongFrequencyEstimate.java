package com.splicemachine.stats.frequency;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface LongFrequencyEstimate extends FrequencyEstimate<Long> {

    long value();
}
