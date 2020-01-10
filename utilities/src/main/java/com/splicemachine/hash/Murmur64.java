/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.primitives.LittleEndianBits;

import java.nio.ByteBuffer;

/**
 * Implementation of the 64-bit version of MurmurHash2.
 *
 * @author Scott Fines
 * Date: 11/23/13
 */
final class Murmur64 implements Hash64 {
    private static final long m = 0xc6a4a7935bd1e995L;
    private static final int r = 47;

    private final int seed;

    Murmur64(int seed) { this.seed = seed; }

    @Override
    public long hash(String elem) {
        assert elem!=null: "Cannot hash a null element";
        int length = elem.length();
        long h = initialize(seed,length);
        int pos =0;
        char[] chars = elem.toCharArray();
        while(length-pos>=8){
            h = hash(h, LittleEndianBits.toLong(chars, pos));
            pos+=8;
        }

        h = updatePartial(chars,length-pos,h,pos);
        return finalize(h);
    }

    @Override
    public long hash(byte[] data, int offset, int length) {
        long h = initialize(seed, length);

        int pos = offset;
        while((length-pos)>=8){
            h = updateFull(data, h, pos);
            pos+=8;
        }

        h = updatePartial(data, length-pos, h, pos);
        h = finalize(h);

        return h;
    }

    @Override
    public long hash(ByteBuffer byteBuffer) {
        int length = byteBuffer.remaining();
        long h = initialize(seed, length);

        byte[] block = new byte[8];
        while(byteBuffer.remaining()>=8){
            byteBuffer.get(block);
            h = updateFull(block, h, 0);
        }

        length = byteBuffer.remaining();
        byteBuffer.get(block,0,length);
        h = updatePartial(block,length,h,0);

        return finalize(h);
    }

    @Override
    public long hash(long element) {
        return finalize(hash(initialize(seed, 8),Long.reverseBytes(element)));
    }

    @Override
    public long hash(int element) {
        long h = initialize(seed,4);
        long e =Integer.reverseBytes(element)&0x00000000ffffffffl;
        h ^= e;
        return finalize(h);
    }

    @Override
    public long hash(float element) {
        //TODO -sf- implement correctly!
        return hash(Float.floatToRawIntBits(element));
    }

    @Override
    public long hash(double element) {
        //TODO -sf- implement correctly!
        return hash(Double.doubleToRawLongBits(element));
    }

    /*********************************************************************************************/
		/*Private helper functions*/

    private long initialize(long seed, int length) {
        return seed^(length*m);
    }

    private long updatePartial(byte[] data, int length, long h, int dataPosition) {
        switch (length) {
            case 7: h ^= ((long)data[dataPosition+6] & 0xff) <<48;
            case 6: h ^= ((long)data[dataPosition+5] & 0xff) <<40;
            case 5: h ^= ((long)data[dataPosition+4] & 0xff) <<32;
            case 4: h ^= ((long)data[dataPosition+3] & 0xff) <<24;
            case 3: h ^= ((long)data[dataPosition+2] & 0xff) <<16;
            case 2: h ^= ((long)data[dataPosition+1] & 0xff) <<8;
            case 1: h ^= ((long)data[dataPosition  ] & 0xff);
        }
        return h;
    }

    private long updatePartial(char[] data, int length, long h, int dataPosition) {
        switch (length) {
            case 7: h ^= ((long)data[dataPosition+6] & 0xff) <<48;
            case 6: h ^= ((long)data[dataPosition+5] & 0xff) <<40;
            case 5: h ^= ((long)data[dataPosition+4] & 0xff) <<32;
            case 4: h ^= ((long)data[dataPosition+3] & 0xff) <<24;
            case 3: h ^= ((long)data[dataPosition+2] & 0xff) <<16;
            case 2: h ^= ((long)data[dataPosition+1] & 0xff) <<8;
            case 1: h ^= ((long)data[dataPosition  ] & 0xff);
        }
        return h;
    }

    private long updateFull(byte[] data, long h, int dataPosition) {
        long k =  LittleEndianBits.toLong(data, dataPosition);

        return hash(h, k);
    }

    private long hash(long h, long k) {
        k *= m;
        k ^= k >>> r;
        k *= m;

        h ^= k;
        h *= m;
        return h;
    }

    private long finalize(long h) {
        h *= m;
        h ^= h>>>r;
        h *= m;
        h ^= h >>> r;
        h *=m;
        return h;
    }
}
