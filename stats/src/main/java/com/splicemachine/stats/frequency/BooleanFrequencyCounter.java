package com.splicemachine.stats.frequency;

import com.splicemachine.stats.BooleanUpdateable;

/**
 * Interface to mark a Boolean-types Frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface BooleanFrequencyCounter extends BooleanUpdateable, FrequencyCounter<Boolean> {

    /**
     * @return the frequency estimates for both true and false. Since Booleans are so
     * limited in size, this will contain all possible elements.
     */
    BooleanFrequentElements frequencies();
}
