package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.LongUpdateable;

/**
 * Marker interface for a Long-specific frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface LongFrequencyCounter extends FrequencyCounter<Long>,LongUpdateable { }
