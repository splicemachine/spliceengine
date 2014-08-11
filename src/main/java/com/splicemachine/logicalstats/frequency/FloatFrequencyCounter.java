package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.FloatUpdateable;

/**
 * Marker interface for a float-specific FrequencyCounter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface FloatFrequencyCounter extends FrequencyCounter<Float>,FloatUpdateable { }
