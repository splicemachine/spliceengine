package com.splicemachine.stats.frequency;

import com.splicemachine.stats.BytesUpdateable;

/**
 * Marker interface for a byte[] specific frequency counter
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface BytesFrequencyCounter extends FrequencyCounter<byte[]>,BytesUpdateable { }
