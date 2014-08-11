package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.ShortUpdateable;

/**
 * Marker interface for a short-specific frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface ShortFrequencyCounter extends FrequencyCounter<Short>,ShortUpdateable { }
