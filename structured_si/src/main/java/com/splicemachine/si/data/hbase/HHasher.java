package com.splicemachine.si.data.hbase;

import com.splicemachine.constants.bytes.HashableBytes;
import com.splicemachine.si.impl.Hasher;

public class HHasher implements Hasher<byte[], HashableBytes> {

    /**
     * Under HBase row keys are byte arrays. This method wraps byte arrays into objects that can be placed into Sets.
     * This allows duplicate requests for a given row to be ignored.
     */
    @Override
    public HashableBytes toHashable(byte[] value) {
        return new HashableBytes(value);
    }

    /**
     * The inverse of the toHashable() method. This recovers the original byte array from its hashable wrapper.
     */
    @Override
    public byte[] fromHashable(HashableBytes value) {
        return value.getBytes();
    }
}
