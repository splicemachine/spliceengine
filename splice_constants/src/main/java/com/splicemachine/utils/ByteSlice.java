package com.splicemachine.utils;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

public class ByteSlice implements Externalizable,Comparable<ByteSlice> {
    private static final Hash32 hashFunction = HashFunctions.murmur3(0);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};
    private byte[] buffer;
    private int offset;
    private int length;
    private transient int hashCode;
    private transient boolean hashSet;

    public ByteSlice() {  }

    public ByteSlice(ByteSlice other) {
        if(other!=null){
            this.buffer = other.buffer;
            this.offset = other.offset;
            this.length = other.length;
            this.hashCode = other.hashCode;
            this.hashSet = other.hashSet;
            assertLengthCorrect(buffer, offset, length);
        }
    }

    public static ByteSlice cachedEmpty(){
        return new CachedByteSlice();
    }

    public static ByteSlice empty(){
        return new ByteSlice(null,0,0);
    }

    public static ByteSlice wrap(ByteBuffer buffer){
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new ByteSlice(data,0,data.length);
    }

    public static ByteSlice cachedWrap(byte[] data){
        return new CachedByteSlice(data);
    }

    /**
     * Equivalent to {@link #wrap(byte[], int, int)}, but caches array copies
     * for efficient memory usage.
     *
     * @param data the data buffer to hold
     * @param offset the offset in the buffer
     * @param length the length of the data block
     * @return a ByteSlice which keeps a cache of the byte array copy
     */
    public static ByteSlice cachedWrap(byte[] data, int offset,int length){
        return new CachedByteSlice(data,offset,length);
    }

    public static ByteSlice wrap(byte[] data, int offset, int length) {
        return new ByteSlice(data,offset,length);
    }

    public static ByteSlice wrap(byte[] rowKey) {
        if(rowKey==null) return new ByteSlice(null,0,0);
        return new ByteSlice(rowKey,0,rowKey.length);
    }

    protected ByteSlice(byte[] buffer, int offset, int length) {
        assertLengthCorrect(buffer, offset, length);
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public byte[] getByteCopy() {
        if(length<=0) return EMPTY_BYTE_ARRAY;
        return BytesUtil.slice(buffer, offset, length);
    }

    public ByteBuffer asBuffer(){
        if(length<=0) return null;
        return ByteBuffer.wrap(buffer,offset,length);
    }

    public void get(byte[] destination, int destOffset){
        assert destOffset + length <=destination.length: "Incorrect size to copy!";
        if(length<=0) return; //nothing to do

        System.arraycopy(buffer,offset,destination,destOffset,length);
    }

    public void get(byte[] destination, int destOffset,int destLength){
        assert destOffset + destLength <=destination.length: "Incorrect size to copy!";
        if(length<=0) return; //nothing to do
        int l = Math.min(destLength,length);

        System.arraycopy(buffer,offset,destination,destOffset,l);
    }

    public void set(byte[] bytes) {
        set(bytes,0,bytes.length);
    }
    public void set(byte[] buffer, int offset, int length) {
        assertLengthCorrect(buffer, offset, length);
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        hashSet=false;
    }

    public void updateSlice(ByteSlice slice, boolean reverse){
        byte[] data = slice.array();
        int dOff = slice.offset();
        int dLen = slice.length();
        if(reverse && dLen>0){
            byte[] copy = new byte[dLen];
            System.arraycopy(data,dOff,copy,0,dLen);
            for(int i=0;i<copy.length;i++){
                copy[i] ^=0xff;
            }
            data = copy;
            dOff = 0;
        }
        set(data, dOff, dLen);
    }
    public void set(ByteSlice rowSlice, boolean reverse) {
        byte[] data;
        int offset, length;

        if(reverse){
            data = rowSlice.data(true);
            offset = 0;
            length = data.length;
        }else{
            data = rowSlice.buffer;
            offset = rowSlice.offset;
            length = rowSlice.length;
        }
        set(data,offset,length);
    }

    public byte[] data(boolean reverse){
        if(length<=0) return EMPTY_BYTE_ARRAY;
        byte[] data = BytesUtil.slice(buffer,offset,length);
        if(reverse){
            for(int i=0;i<data.length;i++){
                data[i] ^=0xff;
            }
        }
        return data;
    }

    public byte[] array() {
        return buffer;
    }

    public void reset(){
        length=0;
        offset=0;
        buffer =null; //allow GC to collect
        hashSet=false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteSlice)) return false;

        ByteSlice that = (ByteSlice) o;
        return equals(that, that.length());
    }

    @Override
    public String toString() {
        return String.format("ByteSlice {buffer=%s}", BytesUtil.toHex(buffer, offset, length));
    }

    public boolean equals(ByteSlice currentData, int equalsLength) {
        if(this.length<=0)
            return currentData.length<=0;
        if(equalsLength!=this.length) return false;
        return equals(currentData.buffer,currentData.offset,currentData.length);
    }

    public boolean equals(byte[] data, int offset, int length) {
        if(this.length<=0)
            return length<=0;
        return Bytes.equals(this.buffer,this.offset,this.length,data,offset,length);
    }

    @Override
    public int compareTo(ByteSlice o) {
        return compareTo(o.buffer,o.offset,o.length);
    }

    public int compareTo(byte[] bytes,int offset, int length) {
        //we need comparisons to occur in an unsigned manner
        return Bytes.compareTo(buffer,this.offset,this.length,bytes,offset,length);
    }

    @Override
    public int hashCode() {
        if(!hashSet) {
            if (buffer == null || length == 0) return 0;
            hashCode = hashFunction.hash(this.buffer, this.offset, this.length);
            hashSet= true;
        }
        return hashCode;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
        offset = in.readInt();
        length = in.readInt();
        buffer = new byte[length];
        if(length > 0) {
            in.readFully(buffer);
        }
        hashSet = false;
        if (buffer.length > 0) {
            assertLengthCorrect(buffer, offset, length);
        } else {
            // If there's nothing in the buffer, reset offset and length
            // to prevent ArrayIndexOutOfBoundsException
            offset = length = 0;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // the deserialized offset MUST be zero, since we're slicing the array
        // using offset as a starting point.
        out.writeInt(0);
        out.writeInt(length);
        if(length > 0) {
            out.write(buffer, offset, length);
        }
        out.flush();
    }

    public void reverse() {
        for(int i=offset;i<offset+length;i++){
            buffer[i]^=0xff;
        }
    }

    public int find(byte toFind, int startOffset){
        if(startOffset<0 || startOffset>=length) return -1;
        int finalOffset = offset+length;
        int position = 0;
        for(int i=offset+startOffset;i<finalOffset;i++){
            if(buffer[i]==toFind) {
                return position;
            }
            position++;
        }
        return -1;
    }

    public void set(ByteSlice newData) {
        set(newData.buffer,newData.offset,newData.length);
    }

    public String toHexString() {
        if(this.length<=0) return "";
        return BytesUtil.toHex(buffer,offset,length);
    }

    private static void assertLengthCorrect(byte[] buffer, int offset, int length) {
        int buffLength = (buffer == null ? 0 : buffer.length);
        assert  (offset + length <= buffLength) : String.format("buffer length, %d, is too short for offset, %d, length, %d", buffLength, offset, length);
    }
}
