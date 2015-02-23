package com.splicemachine.stats.frequency;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public interface BytesFrequencyEstimate extends FrequencyEstimate<byte[]>,Comparable<BytesFrequencyEstimate>{

    ByteBuffer valueBuffer();

    byte[] valueArrayBuffer();

    int valueArrayLength();

    int valueArrayOffset();

    int compare(ByteBuffer buffer);

    int compare(byte[] buffer, int offset,int length);

}
