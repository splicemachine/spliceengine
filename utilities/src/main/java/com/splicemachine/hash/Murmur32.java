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

import com.splicemachine.primitives.LittleEndianBits;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 * Created on: 11/2/13
 */
public class Murmur32 implements Hash32{
    private static final int c1 = 0xcc9e2d51;
    private static final int c2 = 0x1b873593;

    private final int seed;

    Murmur32(int seed) {
        this.seed = seed;
    }

    @Override
    public int hash(String elem) {
        assert elem!=null: "Cannot hash a null element!";
        int h = seed;
        int length = elem.length();
        int pos = 0;
        int visited =0;
        char[] chars = elem.toCharArray();
        while(length-visited>=4){
            /*
             * Since a char has two bytes, we create one int by packing together two chars
             */
            int k1 = LittleEndianBits.toInt(chars, pos);
            h = mutate(h,k1);
            pos+=2;
            visited+=4;
        }
        h = updatePartial(chars,length,pos,h,visited);

        return finalize(h);
    }

    @Override
    public int hash(byte[] bytes, int offset, int length) {
        int pos = offset;
        int visited=0;
        int h = seed;
        while(length-visited>=4){
            int k1 = LittleEndianBits.toInt(bytes, pos);
            h =mutate(h,k1);
            pos+=4;
            visited+=4;
        }
        h = updatePartial(bytes, length, pos,h,visited);
        return finalize(h);
    }

    @Override
    public int hash(long item) {
        long littleEndian = Long.reverseBytes(item);
        int h = seed;
        int k1 = (int)littleEndian;
        h = mutate(h,k1);
        k1 = (int)(littleEndian>>>32);
        h = mutate(h,k1);

        h ^= 8;
        return finalize(h);
    }

    @Override
    public int hash(int element) {
        int h = seed;
        int k1 = Integer.reverseBytes(element);
        h = mutate(h,k1)^4;
        return finalize(h);
    }

    @Override
    public int hash(short element) {
        return hash((int)element);
    }

    @Override
    public int hash(ByteBuffer bytes) {
        int length = bytes.remaining();
        int h = seed;
        byte[] block = new byte[4];
        int bytesVisited=0;
        while(bytes.remaining()>=4){
            bytes.get(block);
            int k1 = LittleEndianBits.toInt(block, 0);
            h =mutate(h, k1);
            bytesVisited+=4;
        }
        bytes.get(block,0,length-bytesVisited);
        h = updatePartial(block,length,0,h,bytesVisited);
        return finalize(h);
    }

    private int updatePartial(CharSequence bytes, int length, int pos, int h,int bytesVisited) {
        int k1 = 0;
        switch(length-bytesVisited){
            case 3:
                k1 ^= (bytes.charAt(pos+2) & 0xFF)<<16;
                bytesVisited++;
            case 2:
                k1 ^= ((bytes.charAt(pos+1) & 0xFF) <<8);
                bytesVisited++;
            case 1:
                k1 ^= bytes.charAt(pos) & 0xFF;
                bytesVisited++;
            default:
                k1 *= c1;
                k1 = Integer.rotateLeft(k1,15);
                k1 *= c2;
                h ^=k1;
        }

        h ^= bytesVisited;
        return h;
    }

    private int updatePartial(char[] bytes, int length, int pos, int h,int bytesVisited) {
        int k1 = 0;
        switch(length-bytesVisited){
            case 3:
                k1 ^= (bytes[pos+2] & 0xFF)<<16;
                bytesVisited++;
            case 2:
                k1 ^= ((bytes[pos+1] & 0xFF) <<8);
                bytesVisited++;
            case 1:
                k1 ^= bytes[pos] & 0xFF;
                bytesVisited++;
            default:
                k1 *= c1;
                k1 = Integer.rotateLeft(k1,15);
                k1 *= c2;
                h ^=k1;
        }

        h ^= bytesVisited;
        return h;
    }

    private int updatePartial(byte[] bytes, int length, int pos, int h,int bytesVisited) {
        int k1 = 0;
        switch(length-bytesVisited){
            case 3:
                k1 ^= (bytes[pos+2] & 0xFF)<<16;
                bytesVisited++;
            case 2:
                k1 ^= ((bytes[pos+1] & 0xFF) <<8);
                bytesVisited++;
            case 1:
                k1 ^= bytes[pos] & 0xFF;
                bytesVisited++;
            default:
                k1 *= c1;
                k1 = Integer.rotateLeft(k1,15);
                k1 *= c2;
                h ^=k1;
        }

        h ^= bytesVisited;
        return h;
    }

    private int finalize(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    private int mutate(int h1, int k1) {
        k1 *=c1;
        k1 = Integer.rotateLeft(k1,15);
        k1 *=c2;

        h1^=k1;
        h1 = Integer.rotateLeft(h1,13);
        h1 = h1 *5 + 0xe6546b64;
        return h1;
    }

    public static void main(String...args) throws Exception{
        for(int i=0;i<100;i++){
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.asLongBuffer().put(i);
            byte[] bytes = bb.array();
            int byteVersion = new Murmur32(0).hash(bytes,0,bytes.length);
            int bbVersion = new Murmur32(0).hash(bb);
            int longVersion = new Murmur32(0).hash(i);
            if(byteVersion!=bbVersion)
                System.out.printf("Disagreement among bytes! Byte: %d, Buffer:%d, long:%d%n",
                        byteVersion,bbVersion,longVersion);
            else if(longVersion!=byteVersion)
                System.out.printf("Disagreement with long! Byte: %d, Buffer:%d, long:%d%n",
                        byteVersion,bbVersion,longVersion);
        }
    }


}
