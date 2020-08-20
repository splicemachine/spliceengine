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

package com.splicemachine.encoding;

import splice.com.google.common.base.Preconditions;
import com.splicemachine.utils.ByteSlice;
import java.math.BigDecimal;

/**
 * Encode multiple fields into a single byte array.  Encode(X) methods delegate to our XEncoding classes.
 *
 * Spec:
 *
 * -- Fields are followed by a single byte, 0, field delimiter.
 * -- Last field does not get a delimiter.
 * -- Empty fields (encodeEmpty()) occupy zero bytes, but are followed by a delimiter.
 *
 * @author Scott Fines
 *         Created on: 6/10/13
 */
public class MultiFieldEncoder {
    private final byte[][] fields;
    private final int numFields;
    private int currentPos;
    private int currentSize;

    private int markedPos = 0;
    private int markedSize = 0;

    private MultiFieldEncoder(int numFields) {
        fields = new byte[numFields][];
        this.numFields = numFields;
        //initialize ourselves
        reset();
    }

    public static MultiFieldEncoder create(int numFields){
        return new MultiFieldEncoder(numFields);
    }

    public MultiFieldEncoder encodeNext(boolean value){
        encodeNext(value,false);
        return this;
    }

    public MultiFieldEncoder encodeNext(boolean value,boolean desc){
//        assert currentPos<fields.length;
        byte[] next = ScalarEncoding.writeBoolean(value,desc);
        currentSize+=next.length;
        fields[currentPos] = next;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(byte value){
        return encodeNext(value,false);
    }

    public MultiFieldEncoder encodeNext(byte value,boolean desc){
        byte[] next = Encoding.encode(value,desc);
        currentSize+=next.length;
        fields[currentPos] = next;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(short value){
        encodeNext(value,false);
        return this;
    }

    public MultiFieldEncoder encodeNext(short value,boolean desc){
//        assert currentPos<fields.length;
        byte[]  next = ScalarEncoding.writeLong(value,desc);
        currentSize+=next.length;
        fields[currentPos] = next;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(int value){
        encodeNext(value,false);
        return this;
    }

    public MultiFieldEncoder encodeNext(int value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = ScalarEncoding.writeLong(value,desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;

    }

    public MultiFieldEncoder encodeNext(long value){
        encodeNext(value,false);
        return this;
    }

    public MultiFieldEncoder encodeNext(long value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = ScalarEncoding.writeLong(value,desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(float value){
        return encodeNext(value,false);
    }

    public MultiFieldEncoder encodeNext(float value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = FloatEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;

    }

    public MultiFieldEncoder encodeNext(double value){
//        assert currentPos<fields.length;
        byte[] bytes = DoubleEncoding.toBytes(value, false);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(double value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = DoubleEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(BigDecimal value){
//        assert currentPos<fields.length;
        byte[] bytes = BigDecimalEncoding.toBytes(value, false);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(BigDecimal value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = BigDecimalEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(String value){
//        assert currentPos<fields.length;
        byte[] bytes = StringEncoding.toBytes(value, false);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(String value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = StringEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }


    public MultiFieldEncoder encodeNext(byte[] value){
        return encodeNext(value,false);
    }

    public MultiFieldEncoder encodeNext(byte[] value,boolean desc){
//        assert currentPos<fields.length;
        byte[] encode = ByteEncoding.encode(value, desc);
        currentSize+=encode.length;
        fields[currentPos] = encode;
        currentPos++;
        return this;
    }

    public byte[] build(){
        //if you haven't tried to encode anything, return empty array
        if(currentPos==0) return Encoding.EMPTY_BYTE_ARRAY;

        byte[] data = new byte[currentSize+currentPos-1];
        int destPos=0;
        for(int srcPos=0;srcPos<currentPos;srcPos++){
            byte[] src = fields[srcPos];
            if(srcPos != 0){
                data[destPos] = 0x00; //we know that 0x00 is never allowed, so it's a safe terminator
                destPos++;
            }
            if(src==null || src.length==0) {
                /* Happens when encodeEmpty() is called, or if a field's encoding is an empty byte array (e.g. null String) */
                continue;
            }

            System.arraycopy(src,0,data,destPos,src.length);
            destPos+=src.length;
        }
        return data;
    }

    public void mark() {
        markedPos = currentPos;
        markedSize = currentSize;
    }

    public void reset(){
        /*
         * Rather than waste time manually clearing data, just write over the positions
         * as we need to. Any remaining garbage in the array will get destroyed along with this object
         * then. Just make sure we don't keep one of these around for forever without using it repeatedly.
         */
        currentPos=markedPos;
        currentSize= markedSize;
    }

    public byte[] getEncodedBytes(int position) {
        Preconditions.checkArgument(position<currentPos,"No bytes available in the current encoder");
        return fields[position];
    }

    /**
     * Directly set raw bytes into the resulting array.
     *
     * This is a dangerous method, because it won't deal with terminators appropriately. If
     * the passed in byte[] contains 0x00 somewhere in the array, then decoding may not
     * work correctly.
     *
     * Thus, this is to be used <em>only</em> when one of the following conditions holds:
     *
     * 1. It is known at all times <em>exactly</em> how long the bytes being set are (that is,
     * it is known to always be 8 bytes, or something). This typically occurs when prepending
     * a UUID or some other such known byte[].
     * 2. It is known that the bytes are <em>already correctly encoded</em>. This occurs when
     * it is known that the bytes have been encoded by a separate operation.
     *
     * @param bytes the bytes to set
     * @return an encoder that can be used to encode the next fields
     */
    public MultiFieldEncoder setRawBytes(byte[] bytes) {
        setRawBytes(bytes,0,bytes==null?0:bytes.length);
        return this;
    }

    /**
     * Directly set raw bytes into the resulting array.
     *
     * This is a dangerous method, because it won't deal with terminators appropriately. If
     * the passed in byte[] contains 0x00 somewhere in the array, then decoding may not
     * work correctly.
     *
     * Thus, this is to be used <em>only</em> when one of the following conditions holds:
     *
     * 1. It is known at all times <em>exactly</em> how long the bytes being set are (that is,
     * it is known to always be 8 bytes, or something). This typically occurs when prepending
     * a UUID or some other such known byte[].
     * 2. It is known that the bytes are <em>already correctly encoded</em>. This occurs when
     * it is known that the bytes have been encoded by a separate operation.
     *
     * @param value the bytes to set
     * @param offset the start of the bytes to set
     * @param length the length of the bytes to set
     * @return an encoder that can be used to encode the next fields
     */
    public MultiFieldEncoder setRawBytes(byte[] value, int offset, int length){
        assert currentPos < numFields;
        if(value==null||length==0){
            currentPos++;
            return this;
        }
        byte[] copy = new byte[length];
        System.arraycopy(value,offset,copy,0,length);
        fields[currentPos] = copy;
        currentPos++;
        currentSize+=length;
        return this;
    }

    public MultiFieldEncoder encodeEmpty() {
        fields[currentPos] = null;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeEmptyFloat() {
        return setRawBytes(Encoding.encodedNullFloat());
    }

    public MultiFieldEncoder encodeEmptyDouble(){
        return setRawBytes(Encoding.encodedNullDouble());
    }

    public int getNumFields() {
        return fields.length;
    }

    public MultiFieldEncoder setRawBytes(ByteSlice slice) {
        return setRawBytes(slice.array(),slice.offset(),slice.length());
    }

    public MultiFieldEncoder encodeNextUnsorted(ByteSlice destTable) {
        return encodeNextUnsorted(destTable.array(),destTable.offset(),destTable.length());
    }

    public MultiFieldEncoder encodeNextUnsorted(byte[] array, int offset, int length) {
        //append a length field
        byte[] total = Encoding.encodeBytesUnsorted(array,offset,length);

        currentSize+=total.length;
        fields[currentPos] = total;
        currentPos++;
        return this;
    }

    /**
     * WARNING: This encoding is <em>not</em> sortable, and will <em>not</em> retain
     * the sort order of the original byte[]. Only use this if sorting that byte[] is unnecessary.
     *
     * @param value the value to be encoded
     * @return a MultiFieldEncoder with {@code value} set in the next available
     * position.
     */
    public MultiFieldEncoder encodeNextUnsorted(byte[] value){
        return encodeNextUnsorted(value,0,value.length);
    }

}
