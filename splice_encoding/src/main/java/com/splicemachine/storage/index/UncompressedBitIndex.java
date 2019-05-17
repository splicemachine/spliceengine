/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.storage.BitReader;
import com.splicemachine.storage.BitWriter;

import com.carrotsearch.hppc.BitSet;


/**
 * Uncompressed, variable-length BitIndex.
 *
 * This index is represented as follows:
 *
 * The first four bits are header bits
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class UncompressedBitIndex implements BitIndex {
    private final BitSet bitSet;
    private final BitSet scalarFields;
    private final BitSet floatFields;
    private final BitSet doubleFields;

    private UncompressedBitIndex(BitSet bitSet, BitSet scalarFields,BitSet floatFields,BitSet doubleFields) {
        this.bitSet = bitSet;
        this.scalarFields = scalarFields;
        this.floatFields = floatFields;
        this.doubleFields = doubleFields;
    }

    @Override
    public int length() {
        return (int) bitSet.length();
    }

    @Override
    public int cardinality() {
        return (int) bitSet.cardinality();
    }

    @Override
    public int cardinality(int position) {
        int count=0;
        for(int i=bitSet.nextSetBit(0);i>=0&&i<position;i=bitSet.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[encodedSize()];
        bytes[0] = (byte)0x80;
        BitWriter bitWriter = new BitWriter(bytes,0,bytes.length,5,true);
        int lastSetBit =-1;
        for(int setPos=bitSet.nextSetBit(0);setPos>=0;setPos=bitSet.nextSetBit(setPos+1)){
            //skip the distance between setPos and lastSetBit
            bitWriter.skip(setPos-lastSetBit-1);
            /*
             * Because this field is present, we need to use 2 bits to indicate the
             * type information necessary to parse. The format for the type bit is
             *
             * Untyped: 00
             * Double: 01
             * Float: 10
             * Scalar: 11
             */
            if(scalarFields!=null &&scalarFields.get(setPos)){
                bitWriter.set(3);
            }else if(floatFields!=null &&floatFields.get(setPos)){
                bitWriter.set(2);
                bitWriter.skipNext();
            }else if(doubleFields!=null &&doubleFields.get(setPos)){
                bitWriter.setNext();
                bitWriter.skipNext();
                bitWriter.setNext();
            }else{
                bitWriter.setNext();
                bitWriter.skip(2);
            }
            lastSetBit=setPos;
        }
        return bytes;
    }

		@Override
		public int nextSetBit(int currentPosition) {
				return bitSet.nextSetBit(currentPosition);
		}

    @Override
    public int encodedSize() {
        /*
         * The number of bytes goes as follows:
         *
         * you need at least as many bits as the highest 1-bit in the bitSet(equivalent
         *  to bitSet.length()). Because each set bit will have an additional 2-bit "type delimiter"
         *  set afterwords, we need to have 3 bits for every set bit, but 1 for every non-set bit
         *
         * This is equivalent to length()+2*numSetBits().
         *
         * we have 4 available bits in the header, and 7 bits in each subsequent byte (we use a continuation
         * bit).
         */
        int numBits = (int) (bitSet.length()+2*bitSet.cardinality());
        int numBytes = 1;
        numBits-=4;
        if(numBits>0){
           numBytes+=numBits/7;
            if(numBits%7!=0)
                numBytes++;
        }

        return numBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UncompressedBitIndex)) return false;

        UncompressedBitIndex that = (UncompressedBitIndex) o;

        if (!bitSet.equals(that.bitSet)) return false;
        if (doubleFields != null ? !doubleFields.equals(that.doubleFields) : that.doubleFields != null) return false;
        if (floatFields != null ? !floatFields.equals(that.floatFields) : that.floatFields != null) return false;
        if (scalarFields != null ? !scalarFields.equals(that.scalarFields) : that.scalarFields != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bitSet.hashCode();
        result = 31 * result + (scalarFields != null ? scalarFields.hashCode() : 0);
        result = 31 * result + (floatFields != null ? floatFields.hashCode() : 0);
        result = 31 * result + (doubleFields != null ? doubleFields.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                bitSet +
                "," + scalarFields +
                "," + floatFields +
                "," + doubleFields +
                '}';
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return this.bitSet.intersects(bitSet);
    }

    @Override
    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    @Override
    public boolean isScalarType(int position) {
        return scalarFields != null && scalarFields.get(position);
    }

    @Override
    public boolean isDoubleType(int position) {
        return doubleFields != null && doubleFields.get(position);
    }

    @Override
    public boolean isFloatType(int position) {
        return floatFields != null && floatFields.get(position);
    }

    @Override
    public BitSet getScalarFields() {
        return scalarFields;
    }

    @Override
    public BitSet getDoubleFields() {
        return doubleFields;
    }

    @Override
    public BitSet getFloatFields() {
        return floatFields;
    }

    @Override
    public BitSet getFields() { return bitSet; }

    @Override
    public BitSet and(BitSet bitSet) {
        final BitSet result = (BitSet) this.bitSet.clone();
        result.and(bitSet);
        return result;
    }

		public static BitIndex create(BitSet setCols,BitSet lengthDelimitedFields,BitSet floatFields,BitSet doubleFields) {
        return new UncompressedBitIndex(setCols,lengthDelimitedFields,floatFields,doubleFields);
    }

    public static BitIndex wrap(byte[] data, int position, int limit) {
        //create a BitSet underneath
        BitSet bitSet = new BitSet();
        BitSet scalarFields = new BitSet();
        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();
        BitReader bitReader = new BitReader(data,position,limit,5,true);

        int bitPos = 0;
        while(bitReader.hasNext()){
            int zeros = bitReader.nextSetBit();
            if(zeros<0) break;
            bitPos+= zeros;
            bitSet.set(bitPos);
            if(bitReader.next()!=0){
                //either float or scalar
                if(bitReader.next()!=0){
                    scalarFields.set(bitPos);
                }else
                    floatFields.set(bitPos);
            }else{
                //either a double or untyped
                if(bitReader.next()!=0)
                    doubleFields.set(bitPos);
            }
            bitPos++;
        }
        return new UncompressedBitIndex(bitSet,scalarFields,floatFields,doubleFields);
    }
}
