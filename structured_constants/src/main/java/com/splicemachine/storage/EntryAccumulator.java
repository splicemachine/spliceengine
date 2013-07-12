package com.splicemachine.storage;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public interface EntryAccumulator {
    void add(int position, ByteBuffer buffer);

    void addScalar(int position, ByteBuffer buffer);

    void addFloat(int position, ByteBuffer buffer);

    void addDouble(int position, ByteBuffer buffer);

    BitSet getRemainingFields();

    byte[] finish();

    void reset();
}
