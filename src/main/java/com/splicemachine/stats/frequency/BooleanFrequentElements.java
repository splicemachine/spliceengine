package com.splicemachine.stats.frequency;


/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface BooleanFrequentElements extends FrequentElements<Boolean> {

    BooleanFrequencyEstimate equalsTrue();

    BooleanFrequencyEstimate equalsFalse();

    BooleanFrequencyEstimate equals(boolean value);
}
