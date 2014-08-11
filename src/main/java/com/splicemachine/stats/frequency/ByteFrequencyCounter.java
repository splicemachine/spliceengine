package com.splicemachine.stats.frequency;

import com.splicemachine.stats.ByteUpdateable;

/**
 * Marker interface to denote a byte-specific frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface ByteFrequencyCounter extends ByteUpdateable, FrequencyCounter<Byte> { }
