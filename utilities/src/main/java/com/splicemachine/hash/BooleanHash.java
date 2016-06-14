package com.splicemachine.hash;

import java.nio.ByteBuffer;

/**
 * A hash function which takes on a boolean value.
 *
 * @author Scott Fines
 * Date: 10/7/14
 */
public interface BooleanHash {

    boolean hash(byte[] value, int offset, int length);

    boolean hash(ByteBuffer bytes);

    boolean hash(long value);

    boolean hash(int value);

}
