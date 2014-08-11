package com.splicemachine.stats.frequency;

import com.splicemachine.stats.FloatUpdateable;

/**
 * Marker interface for a float-specific FrequencyCounter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface FloatFrequencyCounter extends FrequencyCounter<Float>,FloatUpdateable { }
