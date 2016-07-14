/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.encoding;

import org.sparkproject.guava.hash.HashFunction;
import org.sparkproject.guava.hash.Hasher;
import org.sparkproject.guava.hash.Hashing;

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
