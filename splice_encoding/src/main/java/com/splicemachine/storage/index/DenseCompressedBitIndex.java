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
import java.util.Arrays;
import com.carrotsearch.hppc.BitSet;


/**
 * Represents a Dense, Compressed BitSet.
 *
 * BitSets are compressed during the Encoding process using Run-Length Encoding, which is a
 * mechanism where each entry is stored, followed by a count of how many times it's duplicated. For example,
 * the bit sequence 0 11 000 11111 0 1 0 11 can be represented as 01 12 03 15 01 11 01 12 (E.g. the value,
 * followed by a count of how many times that value is repeated).
 *
 * This is often very efficient when there are highly skewed distributions within the BitSet--say, all
 * 1s at the beginning, and all zeros at the end, or some similar structure.
 *
 * Because there is no clear demarcation between the end of a Dense, Compressed bit index and the
 * start of another bitstream within the same stream/buffer, this implementation places a 1 into the
 * Most significant bit (leftmost bit) for every byte after the header.  This way, every byte is guaranteed
 * to be non-zero, at the expense of a slightly larger index.
 *
 * The counts are encoded using Elias Delta Encoding (which has nice size features, plus is re-used from
 * sparse implementations).
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class DenseCompressedBitIndex implements BitIndex {
    private final BitSet bitSet;
    private final BitSet scalarFields;
    private final BitSet floatFields;
    private final BitSet doubleFields;

    private byte[] encodedData;

    DenseCompressedBitIndex(BitSet bitSet,BitSet scalarFields,BitSet floatFields,BitSet doubleFields){
        this.bitSet = bitSet;
        this.scalarFields =scalarFields;
        this.floatFields = floatFields;
        this.doubleFields = doubleFields;
    }

    public static BitIndex compress(BitSet bitSet, BitSet scalarFields,BitSet floatFields,BitSet doubleFields){
				return new DenseCompressedBitIndex(bitSet,scalarFields,floatFields,doubleFields);
    }

    public int length() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        if(encodedData!=null) return encodedData;

        encodedData = new byte[encodedSize()];
        encodedData[0] = (byte)0xC0;

        BitWriter bitWriter = new BitWriter(encodedData,0,encodedData.length,5,true);

        int lastSetBit = -1;
        int numScalars=0;
        int numFloats=0;
        int numDoubles=0;
        int numUntyped=0;
        for(int nextSetBit = bitSet.nextSetBit(0);nextSetBit>=0;nextSetBit=bitSet.nextSetBit(nextSetBit+1)){
            int numZeros = nextSetBit-lastSetBit-1;

            if(numZeros>0){
                writeTypedData(bitWriter,numScalars,numFloats,numDoubles,numUntyped);
                numScalars=0;
                numFloats=0;
                numDoubles=0;
                numUntyped = 0;
                //we have a run of unset values
                bitWriter.skipNext();
                DeltaCoding.encode(numZeros,bitWriter);
            }

            //get our new type
            if(scalarFields.get(nextSetBit)){
                if(numScalars==0){
                    writeTypedData(bitWriter, numScalars, numFloats, numDoubles, numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numScalars++;
            }else if(floatFields.get(nextSetBit)){
                if(numFloats==0){
                    writeTypedData(bitWriter, numScalars, numFloats, numDoubles, numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numFloats++;
            }else if(doubleFields.get(nextSetBit)){
                if(numDoubles==0){
                    writeTypedData(bitWriter, numScalars, numFloats, numDoubles, numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numDoubles++;
            }else{
                if(numUntyped==0){
                    writeTypedData(bitWriter, numScalars, numFloats, numDoubles, numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numUntyped++;
            }

            lastSetBit = nextSetBit;
        }
        writeTypedData(bitWriter,numScalars,numFloats,numDoubles,numUntyped);

        return encodedData;
    }

    private void writeTypedData(BitWriter bitWriter, int numScalars, int numFloats, int numDoubles, int numUntyped) {
        int count=0;
        if(numScalars>0){
            bitWriter.set(3);
            count=numScalars;
        }else if(numFloats>0){
            bitWriter.set(2);
            bitWriter.skipNext();
            count = numFloats;
        }else if(numDoubles>0){
            bitWriter.setNext();
            bitWriter.skipNext();
            bitWriter.setNext();
            count=numDoubles;
        }else if(numUntyped>0){
            bitWriter.setNext();
            bitWriter.skip(2);
            count=numUntyped;
        }
        if(count>0)
            DeltaCoding.encode(count,bitWriter);
    }

    @Override
    public int encodedSize() {
        if(encodedData!=null) return encodedData.length;

        int lastSetBit=-1;
        int numScalars = 0;
        int numFloats = 0;
        int numDoubles = 0;
        int numUntyped = 0;
        int numBits=0;
        for(int nextSetBit = bitSet.nextSetBit(0);nextSetBit>=0;nextSetBit=bitSet.nextSetBit(nextSetBit+1)){
            int numZeros = nextSetBit-lastSetBit-1;
            if(numZeros>0){
                numBits += countTypedData(numScalars, numFloats, numDoubles, numUntyped);
                numScalars=0;
                numFloats=0;
                numDoubles=0;
                numUntyped=0;
                numBits+=DeltaCoding.getEncodedLength(numZeros)+1;
            }
            if(scalarFields!=null && scalarFields.get(nextSetBit)){
                if(numScalars==0){
                    //a change in type
                    numBits +=countTypedData(numScalars,numFloats,numDoubles,numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numScalars++;
            }else if(floatFields!=null && floatFields.get(nextSetBit)){
                if(numFloats==0){
                    //a change in type
                    numBits +=countTypedData(numScalars,numFloats,numDoubles,numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numFloats++;
            }else if(doubleFields!=null && doubleFields.get(nextSetBit)){
                if(numDoubles==0){
                    //a change in type
                    numBits +=countTypedData(numScalars,numFloats,numDoubles,numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numDoubles++;
            }else{
                if(numUntyped==0){
                    //a change in type
                    numBits +=countTypedData(numScalars,numFloats,numDoubles,numUntyped);
                    numScalars=0;
                    numFloats=0;
                    numDoubles=0;
                    numUntyped=0;
                }
                numUntyped++;
            }
            lastSetBit=nextSetBit;
        }
        numBits+=countTypedData(numScalars,numFloats,numDoubles,numUntyped);

        int length = numBits-4; //fit four bits into the header
        int numBytes = length/7;
        if(length%7!=0){
            numBytes++;
        }
        numBytes++;

        return numBytes;
    }

    private int countTypedData(int numScalars, int numFloats, int numDoubles, int numUntyped) {
        int count;
        if(numScalars>0)
            count=numScalars;
        else if(numFloats>0)
            count=numFloats;
        else if(numDoubles>0)
            count = numDoubles;
        else
            count = numUntyped;
        if(count>0)
            return 3+ DeltaCoding.getEncodedLength(count);
        else
            return 0;
    }

    @Override
    public int nextSetBit(int position) {
        return bitSet.nextSetBit(position);
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return this.bitSet.intersects(bitSet);
    }

    @Override
    public BitSet and(BitSet bitSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    @Override
    public boolean isScalarType(int position) {
        return scalarFields.get(position);
    }

    @Override
    public boolean isDoubleType(int position) {
        return doubleFields.get(position);
    }

    @Override
    public boolean isFloatType(int position) {
        return floatFields.get(position);
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
    public String toString() {
        return "{bitSet=" +
                bitSet +
                ", scalarFields=" + scalarFields +
                ", floatFields=" + floatFields +
                ", doubleFields=" + doubleFields +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DenseCompressedBitIndex)) return false;

        DenseCompressedBitIndex that = (DenseCompressedBitIndex) o;

				return bitSet.equals(that.bitSet) &&
								doubleFields.equals(that.doubleFields)&&
								floatFields.equals(that.floatFields) &&
								scalarFields.equals(that.scalarFields);

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

    public static BitIndex wrap(byte[] data, int offset, int length) {

        BitReader reader = new BitReader(data,offset,length,5,true);
        BitSet bitSet = new BitSet();
        BitSet lengthDelimitedFields = new BitSet();
        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();

        int lastSetPos=0;

        while(reader.hasNext()){
            if(reader.next()==0){
                //reading a sequence of unset values
                int next = DeltaCoding.decode(reader);
                if(next<0)
                    break;
                else
                    lastSetPos+=next;
            }else{
                //get the type from the next two bits
                if(reader.next()!=0){
                    //either a float or a scalar
                    if(reader.next()!=0){
                        //scalar type
                        int numScalars = DeltaCoding.decode(reader);
                        bitSet.set(lastSetPos,lastSetPos+numScalars);
                        lengthDelimitedFields.set(lastSetPos,lastSetPos+numScalars);
                        lastSetPos+=numScalars;
                    }else{
                        //float type
                        int count = DeltaCoding.decode(reader);
                        bitSet.set(lastSetPos,lastSetPos+count);
                        floatFields.set(lastSetPos,lastSetPos+count);
                        lastSetPos+=count;
                    }
                }else{
                    if(reader.next()!=0){
                        int numDoubles = DeltaCoding.decode(reader);
                        bitSet.set(lastSetPos,lastSetPos+numDoubles);
                        doubleFields.set(lastSetPos,lastSetPos+numDoubles);
                        lastSetPos+=numDoubles;
                    }else{
                        int numUntyped = DeltaCoding.decode(reader);
                        bitSet.set(lastSetPos,lastSetPos+numUntyped);
                        lastSetPos+=numUntyped;
                    }
                }
            }
        }

        return new DenseCompressedBitIndex(bitSet,lengthDelimitedFields,floatFields,doubleFields);
    }
    
		public static void main(String... args) throws Exception{
        BitSet set = new BitSet();
        set.set(0);
//        set.set(1);
        set.set(2);
        set.set(3);
        set.set(4);
        set.set(5);
        set.set(6);
        set.set(7);
//        set.set(8);

        BitSet lengthFields = new BitSet();
        lengthFields.set(0);
//        lengthFields.set(1);
        lengthFields.set(2);
//        lengthFields.set(3);
//        lengthFields.set(4);
        lengthFields.set(6);
//        lengthFields.set(8);

        BitSet floatFields = null;
//        floatFields = new BitSet();
//        floatFields.set(5);

        BitSet doubleFields = new BitSet();
        doubleFields.set(3);
        doubleFields.set(4);

        BitIndex index = new DenseCompressedBitIndex(set,lengthFields,floatFields,doubleFields);
        byte[] data  = index.encode();
        System.out.println(Arrays.toString(data));
        BitIndex decoded = BitIndexing.compressedBitMap(data,0,data.length);
        for(int i=decoded.nextSetBit(0);i>=0;i=decoded.nextSetBit(i+1));
        System.out.println(decoded);
    }

}
