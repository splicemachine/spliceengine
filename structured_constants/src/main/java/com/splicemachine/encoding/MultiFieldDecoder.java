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
    private int currentOffset;
    private long[] intValueLength;

    private MultiFieldDecoder(){
        this.currentOffset=-1;
    }

    public static MultiFieldDecoder create(){
        return new MultiFieldDecoder();
    }

    public MultiFieldDecoder set(byte[] newData){
        this.data = newData;
        currentOffset = 0;
        return this;
    }

    public void reset(){
        currentOffset=0; //reset to start
    }

    public byte decodeNextByte(){
        return decodeNextByte(false);
    }

    public byte decodeNextByte(boolean desc){
        assert currentOffset<data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return 0;
        }

        byte elem =Encoding.decodeByte(data,currentOffset,desc);
        currentOffset+=2; //skip the terminator
        return elem;
    }

    public short decodeNextShort(){
        return decodeNextShort(false);
    }

    public short decodeNextShort(boolean desc){
        return (short)decodeNextLong(desc);
    }

    public int decodeNextInt(){
        return decodeNextInt(false);
    }

    public int decodeNextInt(boolean desc){
        return (int)decodeNextLong(desc);
    }

    public long decodeNextLong(){
        return decodeNextLong(false);
    }

    public long decodeNextLong(boolean desc){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return 0;
        }

        if(intValueLength==null)
            intValueLength = new long[2];
        Encoding.decodeLongWithLength(data,currentOffset,desc,intValueLength);
        currentOffset+=intValueLength[1]+1;
        return intValueLength[0];
    }

    public float decodeNextFloat(){
        return decodeNextFloat(false);
    }

    public float decodeNextFloat(boolean desc){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return 0f;
        }

        float next = Encoding.decodeFloat(data,currentOffset,desc);
        adjustOffset(5);
        return next;
    }

    public double decodeNextDouble(){
        return decodeNextDouble(false);
    }

    public double decodeNextDouble(boolean desc){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return 0d;
        }

        double next = Encoding.decodeDouble(data,currentOffset,desc);
        currentOffset+=9;
//        adjustOffset(9);
        return next;
    }

    public BigDecimal decodeNextBigDecimal(){
        return decodeNextBigDecimal(false);
    }

    public BigDecimal decodeNextBigDecimal(boolean desc){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return null;
        }

        int offset = currentOffset;
        adjustOffset(-1);

        BigDecimal next = Encoding.decodeBigDecimal(data,offset,currentOffset-offset-1,desc);
        return next;
    }

    public String decodeNextString(){
       return decodeNextString(false);
    }

    public String decodeNextString(boolean desc) {
        assert currentOffset <= data.length;

        if (currentOffset >= 0 &&
                (data.length == currentOffset || data[currentOffset] == 0x00)) {
            currentOffset++;
            return null;
        }

        //determine the length of the string ahead of time
        int offset = currentOffset >= 0 ? currentOffset : 0;
        adjustOffset(-1);
        //the string length is the number of bytes that we encode
        return Encoding.decodeString(data, offset, currentOffset - offset - 1, desc);
    }

    public byte[] decodeNextBytes(){
        return decodeNextBytes(false);
    }

    public byte[] decodeNextBytes(boolean desc){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return new byte[]{};
        }
        byte[] decoded= Encoding.decodeBytes(data,currentOffset,desc);
        currentOffset+=decoded.length+1;
        return decoded;
    }

    public byte[] decodeNextBytesUnsorted(){
        assert currentOffset < data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return new byte[]{};
        }

        if(intValueLength==null)
            intValueLength = new long[2];
        Encoding.decodeLongWithLength(data, currentOffset, false,intValueLength);
        int length = (int)intValueLength[0];
        currentOffset+=intValueLength[1]+1;
        byte[] copy = new byte[length];
        System.arraycopy(data,currentOffset,copy,0,length);
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
    public byte[] getNextRaw(){
        //seek to the next terminator
        if(currentOffset>=data.length) return null;

        if(currentOffset>=0&&data[currentOffset]==0x00) {
            currentOffset++;
            return null;
        }
        int offset = currentOffset>=0?currentOffset:0;
        adjustOffset(-1);

        int length = currentOffset-offset-1;

        byte[] bytes = new byte[length];
        System.arraycopy(data,offset,bytes,0,length);
        return bytes;
    }

    public byte[] getNextRawBytes(){
        if(currentOffset>=data.length) return null;

        if(currentOffset>=0&&data[currentOffset]==0x00) {
            currentOffset++;
            return null;
        }
        int offset = currentOffset>=0?currentOffset:0;
        //read off the length
        int length = Encoding.decodeInt(data,currentOffset,false);
        adjustOffset(5); //adjust the length field
        currentOffset+=length+1; //adjust the data field

        byte[] bytes = new byte[length];
        System.arraycopy(data,offset,bytes,0,length);
        return bytes;
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
            if(currentOffset+i>=data.length){
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
        if(currentOffset>=0&&data[currentOffset]==0x00) {
            currentOffset++;
            return false;
        }
        boolean value = Encoding.decodeBoolean(data,currentOffset);
        currentOffset+=2;
        return value;
    }

    public boolean decodeNextBoolean(boolean desc) {
        if(currentOffset>=0&&data[currentOffset]==0x00) {
            currentOffset++;
            return false;
        }
        boolean value = Encoding.decodeBoolean(data,currentOffset,desc);
        currentOffset+=2;
        return value;
    }

    public static MultiFieldDecoder wrap(byte[] row) {
        MultiFieldDecoder next = new MultiFieldDecoder();
        next.set(row);
        next.reset();
        return next;
    }

    public void skip() {
        //read out raw bytes, and throw them away
        if(currentOffset>=data.length||(currentOffset>=0&&data[currentOffset]==0x00)){
            currentOffset++;
            return;
        }
        adjustOffset(-1);
    }

    /**
     * Gets a slice of the byte[] that encompases the next {@code numFields} fields.
     *
     * @param numFields
     * @return
     */
    public byte[] slice(int numFields) {
        int offset = currentOffset>=0?currentOffset:0;
        int fieldsSkipped = 0;
        while(fieldsSkipped<numFields&&currentOffset<data.length){
            adjustOffset(-1);
            fieldsSkipped++;
        }
        int length = currentOffset-offset-1;
        byte[] retData = new byte[length];
        System.arraycopy(data,offset,retData,0,length);
        return retData;
    }

    public boolean nextIsNull(){
        return currentOffset >= data.length || (currentOffset >= 0 && data[currentOffset] == 0x00);
    }

    public void seek(int newPos) {
        this.currentOffset=newPos;
    }

    public int offset() {
        return currentOffset;
    }

    public byte[] array() {
        return data;
    }
}
