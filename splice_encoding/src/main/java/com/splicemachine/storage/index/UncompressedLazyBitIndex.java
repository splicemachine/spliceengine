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

package com.splicemachine.storage.index;

import java.util.Arrays;
import com.carrotsearch.hppc.BitSet;


/**
 * Lazy implementation of an Uncompressed, Dense BitIndex.
 * @author Scott Fines
 * Created on: 7/8/13
 */
class UncompressedLazyBitIndex extends LazyBitIndex{

    int lastSetBit = -1;

    protected UncompressedLazyBitIndex(byte[] encodedBitMap,
                                       int offset, int length) {
        super(encodedBitMap, offset, length,5);
    }

    @Override
    protected int decodeNext() {
        if(!bitReader.hasNext()) return -1;
        int unsetCount = bitReader.nextSetBit();
        if(unsetCount<0) return -1;
        int pos = lastSetBit+unsetCount+1;
        /*
         * We need the next two bits to determine the type information,
         * which goes as
         *
         * Untyped: 00
         * Double: 01
         * Float: 10
         * Scalar:11
         *
         * A truncated setup for Float or untyped values is not likely (the encoding
         * implementation always sets those bits at the moment), but later improvements may
         * include truncating extra zero bits. Thus, we assume that is possible.
         */
        if(!bitReader.hasNext()){
            //no type bits set, so this must be untyped
            return pos;
        }
        if(bitReader.next()!=0){
            //either float or scalar type
            if(!bitReader.hasNext()){
                //must be 10 = float type
                setFloatField(pos);
                return pos;
            }
            if(bitReader.next()!=0)
                setScalarField(pos);
            else
                setFloatField(pos);
        }else{
            //either a double or untyped
            if(!bitReader.hasNext())
                return pos; //untyped

            if(bitReader.next()!=0)
                setDoubleField(pos);
        }

        lastSetBit=pos;
        return pos;
    }



    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(11);
        bitSet.set(0);
        bitSet.set(1);
//        bitSet.set(3);
//        bitSet.set(4);

        BitSet lengthDelimited = new BitSet();
        lengthDelimited.set(0);

        BitIndex bits = UncompressedBitIndex.create(bitSet,lengthDelimited,null,null);

        byte[] data =bits.encode();
        System.out.println(Arrays.toString(data));

        BitIndex decoded = BitIndexing.uncompressedBitMap(data,0,data.length);
        for(int i=decoded.nextSetBit(0);i>=0;i=decoded.nextSetBit(i+1));

        System.out.println(decoded);
    }

	@Override
	public boolean isCompressed() {
		return false;
	}
}
