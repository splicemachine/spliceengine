package com.splicemachine.encoding;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.util.Bytes;

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
    private int length;
    private long[] intValueLength;

    private int offset;

    private final KryoPool kryoPool;
    private Kryo kryo;

    private MultiFieldDecoder(KryoPool kryoPool){
        this.currentOffset=-1;
        this.kryoPool = kryoPool;
    }

    public static MultiFieldDecoder create(KryoPool kryoPool){
        return new MultiFieldDecoder(kryoPool);
    }

    public void close(){
        if(kryo!=null)
            kryoPool.returnInstance(kryo);
    }

    public MultiFieldDecoder set(byte[] newData){
        return set(newData,0,newData.length);
    }

    public MultiFieldDecoder set(byte[] newData,int offset,int length){
        this.data = newData;
        currentOffset = offset;
        this.length = length;
        this.offset = offset;
        return this;
    }

    public void reset(){
        currentOffset=offset; //reset to start
    }

    public byte decodeNextByte(){
        return decodeNextByte(false);
    }

    public byte decodeNextByte(boolean desc){
        assert currentOffset<offset+length;
        if(currentOffset>=offset &&data[currentOffset]==0x00){
            currentOffset++;
            return 0;
        }

        byte elem =Encoding.decodeByte(data,currentOffset,desc);
        adjustOffset(3);
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
        assert currentOffset < offset+length;
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
        assert currentOffset < offset +length;
        if(nextIsNullFloat()) return 0f;

        float next = Encoding.decodeFloat(data,currentOffset,desc);
        currentOffset+=5;
        return next;
    }

    public double decodeNextDouble(){
        return decodeNextDouble(false);
    }

    public double decodeNextDouble(boolean desc){
        assert currentOffset < offset+length;
        if(nextIsNullDouble()){
            currentOffset+=9;
            return 0d;
        }
        double next = Encoding.decodeDouble(data,currentOffset,desc);
        currentOffset+=9;
        return next;
    }

    public BigDecimal decodeNextBigDecimal(){
        return decodeNextBigDecimal(false);
    }

    public BigDecimal decodeNextBigDecimal(boolean desc){
        assert currentOffset < offset+length && currentOffset<data.length;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return null;
        }

        int oldOffset = currentOffset;
        adjustOffset(-1);

        BigDecimal next = Encoding.decodeBigDecimal(data,oldOffset,currentOffset-oldOffset-1,desc);
        return next;
    }

    public String decodeNextString(){
       return decodeNextString(false);
    }

    public String decodeNextString(boolean desc) {
        assert currentOffset < offset+length;
        if (currentOffset >= offset &&
                (offset+length == currentOffset || data[currentOffset] == 0x00)) {
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
        if(currentOffset>=offset+length) return new byte[]{};
        if(currentOffset>=offset &&data[currentOffset]==0x00){
            currentOffset++;
            return new byte[]{};
        }
        int offset = currentOffset;
        adjustOffset(-1);
        return Encoding.decodeBytes(data,offset,currentOffset-offset-1,desc);
    }

    public byte[] decodeNextBytesUnsorted(){
        assert currentOffset < offset+length;
        if(currentOffset>=offset &&data[currentOffset]==0x00){
            currentOffset++;
            return new byte[]{};
        }

        int offset = currentOffset;
        adjustOffset(-1);
        int length = currentOffset-offset-1;
        return Encoding.decodeBytesUnsortd(data,offset,length);
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
        if(currentOffset>=offset+length) return null;

        if(currentOffset>=offset&&data[currentOffset]==0x00) {
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
        if(currentOffset>=offset+length) return null;
        if(currentOffset>=offset&&data[currentOffset]==0x00) {
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
        if(expectedLength<0){
            expectedLength = offset+length-currentOffset;
        }
        if(expectedLength+currentOffset>=data.length)
            expectedLength = data.length-currentOffset;
        for(int i=1;i<expectedLength;i++){
            if(currentOffset+i>=offset+length){
                //we're out of bytes, so we must have been the end
                currentOffset=offset+length;
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

    public Object decodeNextObject(){
        if(kryo==null)
            kryo = kryoPool.get();

        byte[] bytes = decodeNextBytesUnsorted();
        if(bytes==null||bytes.length==0) return null;

        Input input = new Input(bytes);
        return kryo.readClassAndObject(input);
    }

    public static MultiFieldDecoder wrap(byte[] row,KryoPool kryoPool) {
        return wrap(row,0,row.length,kryoPool);
    }

    public static MultiFieldDecoder wrap(byte[] row,int offset, int length,KryoPool kryoPool) {
        MultiFieldDecoder next = new MultiFieldDecoder(kryoPool);
        next.set(row,offset,length);
        next.reset();
        return next;
    }

    public int skip() {
        //read out raw bytes, and throw them away
        if(currentOffset>=offset+length)
            return 0; //off the end of the array, so nothing to skip

        if((currentOffset>=offset&&data[currentOffset]==0x00)){
            currentOffset++;
            return 1;
        }
        int oldOffset = currentOffset;
        adjustOffset(-1);
        return currentOffset-oldOffset;
    }

    public byte[] slice(int size) {
        int offset = currentOffset>=0?currentOffset:0;
        currentOffset+=size;
        byte[] retData = new byte[size];
        System.arraycopy(data,offset,retData,0,size);
        return retData;
    }

    public boolean nextIsNull(){
        if(currentOffset>=offset+length)
            return true; //we are out of data, so the next field must be null
        else if(currentOffset>=offset){
            return data[currentOffset]==0x00;
        }else return false;
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

    public boolean available() {
        return currentOffset<offset+length;
    }

    public boolean nextIsNullDouble() {
        if(currentOffset>=offset+length) return true;
        //look at the next 8 bytes and see if they equal the double entry
        byte[] nullDouble = Encoding.encodedNullDouble();
        return Bytes.equals(nullDouble,0,nullDouble.length,data,currentOffset,nullDouble.length);
    }

    public boolean nextIsNullFloat(){
        if(currentOffset>=offset+length) return true;
        byte[] nullFloat = Encoding.encodedNullFloat();
        return Bytes.equals(nullFloat,0,nullFloat.length,data,currentOffset,nullFloat.length);
    }

    public int skipDouble() {
        if(currentOffset>=offset+length) return 0;
        int offset = currentOffset;

        byte[] nullDouble = Encoding.encodedNullDouble();
        if(Bytes.equals(nullDouble,0,nullDouble.length,data,offset,nullDouble.length)){
            //skip forward the length of nullDouble+1
            currentOffset+=nullDouble.length+1;
        }else{
            //non-null, so it occupies 8 bytes
            currentOffset+=9;
        }
        return currentOffset-offset;
    }

    public int skipFloat(){
        if(currentOffset>=offset+length) return 0;
        int offset = currentOffset;
        byte[] nullFloat = Encoding.encodedNullFloat();
        if(Bytes.equals(nullFloat,0,nullFloat.length,data,offset,nullFloat.length)){
            //skip forward the length of nullFloat+1
            currentOffset+=nullFloat.length+1;
        }else{
            //non-null, so it occupies 4 bytes
            currentOffset+=5;
        }
        return currentOffset-offset;
    }
}
