package com.splicemachine.derby.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class HashUtils {

    private static HashFunction hasherFactory = Hashing.murmur3_32();

    public static byte hash(byte[][] fields) {
        Hasher h = hasherFactory.newHasher();
        // 0 is the hash byte
        // 1 is the UUID
        // 2 - N are the key fields
        for (byte [] field : fields) {
            if (field != null) h.putBytes(field);             
        }
        return (byte) (h.hash().asBytes()[0] & (byte) 0xf0);
    }
}
