package com.splicemachine.si.data.hbase;

import com.splicemachine.si.impl.Hasher;

import java.nio.ByteBuffer;

public class HHasher implements Hasher<byte[], ByteBuffer> {

    /**
     * Under HBase row keys are byte arrays. This method wraps byte arrays into objects that can be placed into Sets.
     * This allows duplicate requests for a given row to be ignored.
     */
    @Override
    public ByteBuffer toHashable(byte[] value) {
        return ByteBuffer.wrap(value);
    }

    /**
     * The inverse of the toHashable() method. This recovers the original byte array from its hashable wrapper.
     */
    @Override
    public byte[] fromHashable(ByteBuffer value) {
        return value.array();
    }
}
