package com.splicemachine.stats.frequency;

import com.splicemachine.stats.Mergeable;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface BooleanFrequentElements extends FrequentElements<Boolean>,Mergeable<Boolean,BooleanFrequentElements>{

    BooleanFrequencyEstimate equalsTrue();

    BooleanFrequencyEstimate equalsFalse();

    BooleanFrequencyEstimate equals(boolean value);
}
