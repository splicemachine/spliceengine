package com.splicemachine.hbase;

import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.CachedByteSlice;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class KVPair implements Comparable<KVPair> {

    public enum Type{
        INSERT((byte)0x01),
        UPDATE((byte)0x02),
        DELETE((byte)0x03),
        EMPTY_COLUMN((byte)0x04),
        UPSERT((byte)0x05),
        FOREIGN_KEY_CHECK((byte)0x06);

        private final byte typeCode;

        private Type(byte typeCode) { this.typeCode = typeCode; }

        public static Type decode(byte typeByte) {
            for(Type type:values()){
                if(type.typeCode==typeByte) return type;
            }
            throw new IllegalArgumentException("Incorrect typeByte "+ typeByte);
        }

        public byte asByte() {
            return typeCode;
        }
    }

    /*fields*/
    private Type type;
    private ByteSlice rowKey;
    private ByteSlice value;

    /*Factory methods*/
    public static KVPair delete(byte[] rowKey) {
        return new KVPair(ByteSlice.cachedWrap(rowKey), ByteSlice.cachedEmpty(),Type.DELETE);
    }

    /*Constructors*/
    public KVPair(byte[] rowKey, byte[] value){
        this(rowKey,value,Type.INSERT);
    }

    public KVPair(byte[] rowKey, byte[] value, Type type){
        this(ByteSlice.cachedWrap(rowKey),ByteSlice.cachedWrap(value),type);
    }

    public KVPair(byte[] rowKeyBuffer,int rowKeyOffset,int rowKeyLength,
                  byte[] valueBuffer,int valueOffset,int valueLength){
       this(rowKeyBuffer, rowKeyOffset, rowKeyLength, valueBuffer, valueOffset, valueLength,Type.INSERT);
    }

    public KVPair(byte[] rowKeyBuffer,int rowKeyOffset,int rowKeyLength,
                  byte[] valueBuffer,int valueOffset,int valueLength, Type type){
        this(ByteSlice.wrap(rowKeyBuffer,rowKeyOffset,rowKeyLength),
                ByteSlice.wrap(valueBuffer,valueOffset,valueLength),type);
    }

    public KVPair() {
        this(ByteSlice.cachedEmpty(),ByteSlice.cachedEmpty(),Type.INSERT);
    }

    public KVPair(ByteSlice rowKey,ByteSlice value,Type type) {
        assert rowKey!=null: "Cannot create a KVPair without a row key!";
        assert value!=null: "Cannot create a KVPair without a value!";
        this.rowKey = rowKey;
        this.value = value;
        this.type = type;
    }

    /**
     * @return a shallow copy of this KVPair. This will copy over references, but will <em>not</em>
     * move bytes out of any underlying byte arrays (if applicable)
     */
    public KVPair shallowClone(){
       return new KVPair(new CachedByteSlice(rowKey),new CachedByteSlice(value),type);
    }

    /**
     * @return a deep copy of this KVPair. This will copy both references <em>and</em> bytes from
     * the any underlying byte arrays. This is significantly more expensive to perform, but will
     * give you greater copy safety. However, you should only need to do this if you plan on
     * modifying the underlying byte buffers directly, which you should rarely wish to do.
     */
    public KVPair deepClone(){
        return new KVPair(getRowKey(),getValue(),type);
    }

    /**
     * @return the number of bytes held by this KVPair. This is usually the vast majority of
     * the Heap Footprint of an individual KVPair, so use this method as a reasonable approximation
     * of how large this KVPair is on the network and the heap.
     */
    public long getSize() {
        return rowKey.length()+value.length();
    }

    /**
     * @return a slice representing the value in this KVPair. This does not move any data,
     * so it is very cheap to call.
     */
    public ByteSlice valueSlice(){ return value; }

    /**
     * @return a slice representing the row key in this KVPair. Does not move any data, so
     * it's very cheap.
     */
    public ByteSlice rowKeySlice(){ return rowKey; }

    /**
     * Get a <em>copy</em> of the value contained in this KVPair. Do NOT use this when
     * you don't want bytes to move. Use {@link #valueSlice()} instead.
     *
     * @return a copy of the value contained in this KVPair.
     */
    public byte[] getValue(){
        return value.getByteCopy();
    }

    /**
     * Get a <em>copy</em> of the row key contained in this KVPair. Do NOT use this when
     * you don't want bytes to move. Use {@link #valueSlice()} instead.
     *
     * @return a copy of the value contained in this KVPair.
     */
    public byte[] getRowKey(){
        return rowKey.getByteCopy();
    }

    /**
     * @return the type of this KVPair.
     */
    public Type getType(){ return type; }

    /*setters*/
    public void setValue(byte[] value){ this.value.set(value); }
    public void setKey(byte[] key){ this.rowKey.set(key); }
    public void setType(Type type) { this.type = type; }

    @Override
    public int compareTo(KVPair o) {
        if(o==null) return 1;
        return rowKey.compareTo(o.rowKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KVPair)) return false;

        KVPair kvPair = (KVPair) o;

        return type == kvPair.type && rowKey.equals(kvPair.rowKey);
    }

    @Override
    public int hashCode() {
        int result = rowKey.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
