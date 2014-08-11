package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.DoubleUpdateable;

/**
 * Marker interface for a double-specific frequency counter
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface DoubleFrequencyCounter extends FrequencyCounter<Double>,DoubleUpdateable { }
