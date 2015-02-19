package com.splicemachine.primitives;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Abstract representation of a Comparator for byte arrays.
 *
 * This adds an additional method which allows comparing only
 * a portion of the byte arrays (specified by an offset and a length).
 *
 * @author Scott Fines
 * Date: 10/7/14
 */
public interface ByteComparator extends Comparator<byte[]> {

    int compare(byte[] b1, int b1Offset,int b1Length, byte[] b2,int b2Offset,int b2Length);

    int compare(ByteBuffer buffer, byte[] b2,int b2Offset,int b2Length);

    int compare(ByteBuffer lBuffer, ByteBuffer rBuffer);

    boolean equals(byte[] b1, int b1Offset,int b1Length, byte[] b2,int b2Offset,int b2Length);

    boolean equals(ByteBuffer buffer, byte[] b2,int b2Offset,int b2Length);

    boolean equals(ByteBuffer lBuffer, ByteBuffer rBuffer);

    boolean isEmpty(byte[] stop);
}
