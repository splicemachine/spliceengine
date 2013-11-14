package com.splicemachine.encoding;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.splicemachine.utils.kryo.KryoPool;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 * Created on: 6/10/13
 */
public class MultiFieldEncoder {
    private final byte[][] fields;
    private final int numFields;
    private int currentPos;
    private int currentSize;
    private int initialPos;
    private int initalSize;

    private final KryoPool kryoPool;
    private Kryo kryo;

    private MultiFieldEncoder(KryoPool kryoPool,int numFields){
        fields = new byte[numFields][];
        this.numFields = numFields;
        this.initialPos=0;
        this.initalSize=0;
        this.kryoPool = kryoPool;

        //initialize ourselves
        reset();
    }

//    public static MultiFieldEncoder create(int numFields){
//        return new MultiFieldEncoder(KryoPool.defaultPool(),numFields);
//    }

    public static MultiFieldEncoder create(KryoPool kryoPool,int numFields){
        return new MultiFieldEncoder(kryoPool,numFields);
    }

    public void close(){
        if(kryo!=null)
            kryoPool.returnInstance(kryo);
    }


    public MultiFieldEncoder encodeNext(boolean value){
        encodeNext(value,false);
        return this;
    }

    public MultiFieldEncoder encodeNext(boolean value,boolean desc){
//        assert currentPos<fields.length;
        byte[] next = ScalarEncoding.toBytes(value, desc);
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
        byte[]  next = ScalarEncoding.toBytes(value, desc);
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
        byte[] bytes = ScalarEncoding.toBytes(value, desc);
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
        byte[] bytes = ScalarEncoding.toBytes(value, desc);
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
        byte[] bytes = DecimalEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;

    }

    public MultiFieldEncoder encodeNext(double value){
//        assert currentPos<fields.length;
        byte[] bytes = DecimalEncoding.toBytes(value, false);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(double value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = DecimalEncoding.toBytes(value, desc);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(BigDecimal value){
//        assert currentPos<fields.length;
        byte[] bytes = DecimalEncoding.toBytes(value, false);
        currentSize+=bytes.length;
        fields[currentPos] = bytes;
        currentPos++;
        return this;
    }

    public MultiFieldEncoder encodeNext(BigDecimal value,boolean desc){
//        assert currentPos<fields.length;
        byte[] bytes = DecimalEncoding.toBytes(value, desc);
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

    /**
     * WARNING: This encoding is <em>not</em> sortable, and will <em>not</em> retain
     * the sort order of the original byte[]. Only use this if sorting that byte[] is unnecessary.
     *
     * @param value the value to be encoded
     * @return a MultiFieldEncoder with {@code value} set in the next available
     * position.
     */
    public MultiFieldEncoder encodeNextUnsorted(byte[] value){
//        assert currentPos<fields.length;
        //append a length field
        byte[] total = Encoding.encodeBytesUnsorted(value);

        currentSize+=total.length;
        fields[currentPos] = total;
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

    public MultiFieldEncoder encodeNextObject(Object o){
        if(kryo==null)
            kryo = kryoPool.get();
        Output output = new Output(new byte[20],-1);
        kryo.writeClassAndObject(output,o);
        encodeNextUnsorted(output.toBytes());
        return this;
    }

    public byte[] build(){
        //if you haven't tried to encode anything, return empty array
        if(currentPos==0) return new byte[0];

        byte[] data = new byte[currentSize+currentPos-1];
        int destPos=0;
        boolean isFirst = true;
        for(int srcPos=0;srcPos<currentPos;srcPos++){
            byte[] src = fields[srcPos];
            //TODO -sf- should we blow up here instead?
            if(!isFirst){
                data[destPos] = 0x00; //we know that 0x00 is never allowed, so it's a safe terminator
                destPos++;
            } else{
                isFirst=false;
            }
            if(src==null||src.length==0)
                continue;

            System.arraycopy(src,0,data,destPos,src.length);
            destPos+=src.length;
        }
        return data;
    }

    public void reset(){
        /*
         * Rather than waste time manually clearing data, just write over the positions
         * as we need to. Any remaining garbage in the array will get destroyed along with this object
         * then. Just make sure we don't keep one of these around for forever without using it repeatedly.
         */
        currentPos=initialPos;
        currentSize=initalSize;
    }

    public void mark(){
        initialPos=currentPos;
        initalSize=currentSize;
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
}
