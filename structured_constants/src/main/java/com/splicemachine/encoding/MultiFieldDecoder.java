package com.splicemachine.encoding;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;

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

    private MultiFieldDecoder(){
        this.currentOffset=-1;
		}

    public static MultiFieldDecoder create(){
        return new MultiFieldDecoder();
    }

    public static MultiFieldDecoder wrap(byte[] row) {
        return wrap(row,0,row.length);
    }

    public static MultiFieldDecoder wrap(byte[] row, int offset, int length) {
        MultiFieldDecoder next = new MultiFieldDecoder();
        next.set(row,offset,length);
        next.reset();
        return next;
    }

    public void close(){ }

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
        assert available();
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
        if(!available())
            return 0l;
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
        assert available();
        if(nextIsNullFloat()) return 0f;

        float next = Encoding.decodeFloat(data,currentOffset,desc);
        currentOffset+=5;
        return next;
    }

    public double decodeNextDouble(){
        return decodeNextDouble(false);
    }

    public double decodeNextDouble(boolean desc){
        assert available();
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
        assert available();
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return null;
        }

        int oldOffset = currentOffset;
        adjustOffset(-1);

        return Encoding.decodeBigDecimal(data,oldOffset,currentOffset-oldOffset-1,desc);
    }

    public String decodeNextString(){
       return decodeNextString(false);
    }

    public String decodeNextString(boolean desc) {
        assert available();
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
        if(!available()) return new byte[]{};
        if(currentOffset>=offset &&data[currentOffset]==0x00){
            currentOffset++;
            return new byte[]{};
        }
        int offset = currentOffset;
        adjustOffset(-1);
        return Encoding.decodeBytes(data,offset,currentOffset-offset-1,desc);
    }

    public byte[] decodeNextBytesUnsorted(){
        assert available();
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
     * otherwise you may not get the correct array. Use {@link #decodeNextBytesUnsorted()}
     * in that case.
     *
     * @return a view of the next field's bytes[]
     */
    public byte[] getNextRaw(){
        //seek to the next terminator
        if(!available()) return new byte[]{};

        if(currentOffset>=offset&&data[currentOffset]==0x00) {
            currentOffset++;
            return new byte[]{};
        }
        int _offset = currentOffset>=offset?currentOffset:offset;
        adjustOffset(-1);

        int length = currentOffset-_offset-1;

        byte[] bytes = new byte[length];
        System.arraycopy(data,_offset,bytes,0,length);
        return bytes;
    }

    public byte[] getNextRawFloat(){
        if(!available())
            return new byte[]{};
        int offset = currentOffset>=0?currentOffset:0;
        currentOffset+=4;
        byte[] retData = new byte[4];
        System.arraycopy(data,offset,retData,0,4);
        currentOffset++;
        return retData;
    }

    public byte[] getNextRawDouble(){
        if(!available())
            return new byte[]{};
        int offset = currentOffset>=0?currentOffset:0;
        currentOffset+=8;
        byte[] retData = new byte[8];
        System.arraycopy(data,offset,retData,0,8);
        currentOffset++;
        return retData;
    }

    public byte[] getNextRawLong(){
        if(!available())
            return new byte[]{};
        int offset = currentOffset>=0?currentOffset:0;
        int length = ScalarEncoding.toLongLength(data, offset, false);
        currentOffset+=length;
        byte[] retData = new byte[length];
        System.arraycopy(data,offset,retData,0,length);
        currentOffset++;
        return retData;
    }
//    public byte[] getNextRawBytes(){
//        if(!available()) return new byte[]{};
//        if(currentOffset>=offset&&data[currentOffset]==0x00) {
//            currentOffset++;
//            return new byte[]{};
//        }
//        int offset = currentOffset>=0?currentOffset:0;
//        //read off the length
//        Encoding.decodeLongWithLength(data,currentOffset,false,intValueLength);
//        int length = (int)intValueLength[0];
//        int offsetAdjust = (int)intValueLength[1];
//        currentOffset+=offsetAdjust;
//        currentOffset+=length+1; //adjust the data field
//
//        byte[] bytes = new byte[length];
//        System.arraycopy(data,offset,bytes,0,length);
//        return bytes;
//    }

    public boolean decodeNextBoolean() {
        return decodeNextBoolean(false);
    }

    public boolean decodeNextBoolean(boolean desc) {
        assert available();
        if(currentOffset>=0&&data[currentOffset]==0x00) {
            currentOffset++;
            return false;
        }
        boolean value = Encoding.decodeBoolean(data,currentOffset,desc);
        currentOffset+=2;
        return value;
    }

		public int skip() {
        //read out raw bytes, and throw them away
        if(!available())
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
        if(!available())
            return new byte[]{};
        int offset = currentOffset>=0?currentOffset:0;
        currentOffset+=size;
        byte[] retData = new byte[size];
        System.arraycopy(data,offset,retData,0,size);
        return retData;
    }

    public boolean nextIsNull(){
        return !available() || (currentOffset >= offset && data[currentOffset] == 0x00);
    }

    public void seek(int newPos) {
        Preconditions.checkNotNull(newPos<offset+length,"New position is past the end of the data!");
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
        if(!available()) return true;
        //look at the next 8 bytes and see if they equal the double entry
        return check2ByteNull(Encoding.encodedNullDouble());
    }

    public boolean nextIsNullFloat(){
				return !available() || check2ByteNull(Encoding.encodedNullFloat());
		}

    public int skipDouble() {
        if(!available()) return 0;
        int offset = currentOffset;
        if(check2ByteNull(Encoding.encodedNullDouble())){
            //skip forward the length of nullDouble+1
            currentOffset+=Encoding.encodedNullDoubleLength()+1;
        }else{
            //non-null, so it occupies 8 bytes
            currentOffset+=9;
        }
        return currentOffset-offset;
    }

    public int skipFloat(){
        if(!available()) return 0;
        int offset = currentOffset;
        if(check2ByteNull(Encoding.encodedNullFloat())){
            //skip forward the length of nullFloat+1
            currentOffset+=Encoding.encodedNullFloatLength()+1;
        }else{
            //non-null, so it occupies 4 bytes
            currentOffset+=5;
        }
        return currentOffset-offset;
    }
    
    public int skipLong() {
        if(!available())
            return 0;
        if(currentOffset>=0 &&data[currentOffset]==0x00){
            currentOffset++;
            return 0;
        }
        int i = ScalarEncoding.toLongLength(data, currentOffset, false);
        currentOffset+=i+1;        	
        return i;
    }

/*******************************************************************************************************************************************************************/
    /*private helper methods*/
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
    
    private boolean check2ByteNull(byte[] nullValue) {
    	return nullValue[0] == data[currentOffset] && nullValue[1] == data[currentOffset+1];
    }
    
}
