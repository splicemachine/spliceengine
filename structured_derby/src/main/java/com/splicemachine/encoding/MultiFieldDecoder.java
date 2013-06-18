package com.splicemachine.encoding;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Decodes a single byte[] into multiple field based on terminator elements.
 *
 * @author Scott Fines
 * Created on: 6/12/13
 */
public class MultiFieldDecoder {
    private byte[] data;
    private int currentOffset ;

    private MultiFieldDecoder(){
        this.currentOffset=0;
    }

    public static MultiFieldDecoder create(){
        return new MultiFieldDecoder();
    }

    public MultiFieldDecoder set(byte[] newData){
        this.data = newData;
        currentOffset = 0;
        return this;
    }

    public byte decodeNextByte(){
        return decodeNextByte(false);
    }

    public byte decodeNextByte(boolean desc){
        assert currentOffset<data.length;

        byte elem =Encoding.decodeByte(data,currentOffset,desc);
        currentOffset+=2; //skip the terminator
        return elem;
    }

    public short decodeNextShort(){
        return decodeNextShort(false);
    }

    public short decodeNextShort(boolean desc){
        assert currentOffset < data.length;

        short next = Encoding.decodeShort(data,currentOffset,desc);
        //read the bytes to find the next terminator
        //a short should be 1-3 bytes further along
        adjustOffset(3);
        return next;
    }

    public int decodeNextInt(){
        return decodeNextInt(false);
    }

    public int decodeNextInt(boolean desc){
        assert currentOffset < data.length;

        int next = Encoding.decodeInt(data,currentOffset,desc);
        //next encoding should be 1-5 bytes further than the current offset
        adjustOffset(5);
        return next;
    }

    public long decodeNextLong(){
        return decodeNextLong(false);
    }

    public long decodeNextLong(boolean desc){
        assert currentOffset < data.length;

        long next = Encoding.decodeInt(data,currentOffset,desc);
        adjustOffset(9);
        return next;
    }

    public float decodeNextFloat(){
        return decodeNextFloat(false);
    }

    public float decodeNextFloat(boolean desc){
        assert currentOffset < data.length;

        float next = Encoding.decodeFloat(data,currentOffset,desc);
        adjustOffset(5);
        return next;
    }

    public double decodeNextDouble(){
        return decodeNextDouble(false);
    }

    public double decodeNextDouble(boolean desc){
        assert currentOffset < data.length;

        double next = Encoding.decodeDouble(data,currentOffset,desc);
        adjustOffset(9);
        return next;
    }

    public BigDecimal decodeNextBigDecimal(){
        return decodeNextBigDecimal(false);
    }

    public BigDecimal decodeNextBigDecimal(boolean desc){
        assert currentOffset < data.length;

        BigDecimal next = Encoding.decodeBigDecimal(data,currentOffset,desc);
        adjustOffset(-1);
        return next;
    }

    public String decodeNextString(){
       return decodeNextString(false);
    }

    public String decodeNextString(boolean desc){
        assert currentOffset < data.length;

        //determine the length of the string ahead of time
        int offset = currentOffset;
        adjustOffset(-1);
        //the string length is the number of bytes that we encode
        return Encoding.decodeString(data,offset,currentOffset-offset-1,desc);
    }

    public byte[] decodeNextBytes(){
        return decodeNextBytes(false);
    }

    public byte[] decodeNextBytes(boolean desc){
        assert currentOffset < data.length;

        byte[] decoded= Encoding.decodeBytes(data,currentOffset,desc);
        currentOffset+=decoded.length+1;
        return decoded;
    }

    public byte[] decodeNextBytesUnsorted(){
        assert currentOffset < data.length;

        int length = Encoding.decodeInt(data,currentOffset,false);
        byte[] copy = new byte[length];
        System.arraycopy(data,0,copy,currentOffset,length);
        currentOffset+=length+1;
        return copy;
    }

    /**
     * Do <em>not</em> use this when the type is an unsorted byte[],
     * otherwise you may not get the correct array. Use {@link #getNextRawBytes()}
     * in that case.
     *
     * @return a view of the next field's bytes[]
     */
    public ByteBuffer getNextRaw(){
        //seek to the next terminator
        int offset = currentOffset;
        adjustOffset(-1);

        int length = currentOffset-offset-1;
        return ByteBuffer.wrap(data,offset,length);
    }

    public ByteBuffer getNextRawBytes(){
        int offset = currentOffset;
        //read off the length
        int length = Encoding.decodeInt(data,currentOffset,false);
        adjustOffset(5);
        adjustOffset(length);
        return ByteBuffer.wrap(data,offset,offset+length);
    }

    private void adjustOffset(int expectedLength){
        /*
         * if expectedLength <0, then we don't know where
         * the next terminator will be, so just keep looking until
         * we find one or we run out of data
         */
        if(expectedLength<0)
            expectedLength = data.length-currentOffset;
        for(int i=1;i<expectedLength;i++){
            if(currentOffset+i>data.length){
                //we're out of bytes, so we must have been the end
                currentOffset=data.length;
                return;
            }

            byte n = data[currentOffset+i];
            if(n == 0x00){
                currentOffset+=i+1;
                return;
            }
        }
        currentOffset +=expectedLength+1; //not found before the end of the xpectedLength
    }

    public boolean decodeNextBoolean() {
        boolean value = Encoding.decodeBoolean(data,currentOffset);
        currentOffset+=2;
        return value;
    }

    public static void main(String... args) throws Exception{
        MultiFieldEncoder encoder = MultiFieldEncoder.create(4);
        encoder.encodeNext("testing");
        encoder.encodeNext("SYS");
        encoder.encodeNext(1);
        encoder.encodeNext("hello");

        byte[] data = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.create();
        decoder.set(data);
        ByteBuffer b = decoder.getNextRaw();
        System.out.println(Encoding.decodeString(b));
        b = decoder.getNextRaw();
        System.out.println(Encoding.decodeString(b));
        b = decoder.getNextRaw();
        System.out.println(Encoding.decodeInt(b));
        b = decoder.getNextRaw();
        System.out.println(Encoding.decodeString(b));
    }
}
