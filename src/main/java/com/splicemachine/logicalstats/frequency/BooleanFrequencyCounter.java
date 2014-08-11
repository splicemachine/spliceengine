package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.BooleanUpdateable;

/**
 * Interface to mark a Boolean-types Frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface BooleanFrequencyCounter extends BooleanUpdateable, FrequencyCounter<Boolean> { }
