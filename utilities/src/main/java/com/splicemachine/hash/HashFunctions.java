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

package com.splicemachine.hash;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;

/**
 * Utility class for constructing hash functions of various types.
 *
 * @author Scott Fines
 * Date: 11/12/13
 */
public class HashFunctions {

    private HashFunctions(){}

    /**
     * The Same hash function as is used by java.util.HashMap.
     *
     * @return the same hash function as used by java.util.HashMap
     */
    public static Hash32 utilHash(){
        return UtilHash.INSTANCE;
    }

    /**
     * Implementation of Murmur3, in 32-bit mode.
     *
     * @param seed the seed to use
     * @return a 32-bit Murmur3 hash function
     */
    public static Hash32 murmur3(int seed){
        return new Murmur32(seed);
    }

    /**
     * Implementation of Murmur2, in 64-bit mode.
     *
     * @param seed the seed to use
     * @return a 64-bit Murmur2 hash function
     */
    public static Hash64 murmur2_64(int seed){
        return new Murmur64(seed);
    }

    public static BooleanHash booleanHash(int seed){
        final Hash32 hash = new Murmur32(seed);
        return new BooleanHash() {
            @Override
            public boolean hash(byte[] value, int offset, int length) {
                return hash.hash(value,offset,length)%2==0;
            }

            @Override public boolean hash(ByteBuffer bytes) { return hash.hash(bytes)%2==0; }
            @Override public boolean hash(long value) { return hash.hash(value)%2==0; }
            @Override public boolean hash(int value) { return hash.hash(value)%2==0; }
        };
    }

    private static class UtilHash implements Hash32{
        private static final UtilHash INSTANCE = new UtilHash();

        @Override
        public int hash(String elem) {
            assert elem!=null: "Cannot hash a null element!";
            return elem.hashCode();
        }

        @Override
        public int hash(byte[] bytes, int offset, int length) {
            int h = 1;
            int end = offset+length;
            for(int i=offset;i<end;i++){
                h = 31*h + bytes[offset];
            }
            return adjust(h);
        }

        @Override
        public int hash(ByteBuffer buffer) {
            int h =1;
            int end = buffer.remaining();
            for(int i=0;i<end;i++){
                h = 31*h + buffer.get();
            }
            return adjust(h);
        }

        @Override public int hash(long element) { return adjust((int)(element^(element>>>32))); }

        @Override public int hash(int element) { return adjust(element); }

        @Override public int hash(short element) { return adjust((int)element); }

        private int adjust(int h) {
            h ^= (h>>>20)^(h>>>12);
            return h ^(h>>>7)^(h>>>4);
        }
    }
}

