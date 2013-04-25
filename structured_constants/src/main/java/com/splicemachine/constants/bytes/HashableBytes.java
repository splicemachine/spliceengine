package com.splicemachine.constants.bytes;

import java.util.Arrays;

public class HashableBytes {
    private final byte[] bytes;

    public HashableBytes(byte[] b) {
        bytes = new byte[b.length];
        System.arraycopy(b, 0, bytes, 0, b.length);
    }

    public byte[] getBytes() {
        byte[] result = new byte[bytes.length];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashableBytes) {
            HashableBytes other = (HashableBytes) obj;
            return Arrays.equals(bytes, other.bytes);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 19;
        for (int i=0; i<bytes.length; i++) {
            result = 43 * result + bytes[i];
        }
        return result;
    }
}
