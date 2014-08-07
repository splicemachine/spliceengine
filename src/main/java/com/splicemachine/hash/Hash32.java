package com.splicemachine.hash;

import java.nio.ByteBuffer;

/**
 * Representation of a 32-bit hash function.
 *
 * @author Scott Fines
 * Date: 11/12/13
 */
public interface Hash32 {

    public int hash(byte[] bytes, int offset,int length);

    public int hash(ByteBuffer buffer);

    public int hash(long element);

    public int hash(int element);

    public int hash(short element);
}
