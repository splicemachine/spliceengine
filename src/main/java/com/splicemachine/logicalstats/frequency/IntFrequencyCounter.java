package com.splicemachine.logicalstats.frequency;

import com.splicemachine.logicalstats.IntUpdateable;

/**
 * Marker interface for an integer-specific frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface IntFrequencyCounter extends FrequencyCounter<Integer>,IntUpdateable { }
