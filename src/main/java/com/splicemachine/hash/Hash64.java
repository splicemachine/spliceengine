package com.splicemachine.hash;

import java.nio.ByteBuffer;

/**
 * Representation of a 64-bit hash function.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface Hash64 {
    long hash(byte[] bytes, int offset, int length);

    long hash(ByteBuffer byteBuffer);

    long hash(long element);

    long hash(int element);

    long hash(float element);

    long hash(double element);
}
