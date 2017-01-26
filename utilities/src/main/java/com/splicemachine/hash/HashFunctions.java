/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.hash;

import com.splicemachine.annotations.ThreadSafe;

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
    @ThreadSafe
    public static Hash32 utilHash(){
        return UtilHash.INSTANCE;
    }

    /**
     * Implementation of Murmur3, in 32-bit mode.
     *
     * @param seed the seed to use
     * @return a 32-bit Murmur3 hash function
     */
    @ThreadSafe
    public static Hash32 murmur3(int seed){
        return new Murmur32(seed);
    }

    /**
     * Implementation of Murmur2, in 64-bit mode.
     *
     * @param seed the seed to use
     * @return a 64-bit Murmur2 hash function
     */
    @ThreadSafe
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

