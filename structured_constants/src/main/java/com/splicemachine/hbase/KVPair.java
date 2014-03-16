package com.splicemachine.hbase;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class KVPair implements Externalizable,Comparable<KVPair> {

    private static final long serialVersionUID = 2l;
    private byte[] rowKey;
    private byte[] value;
    private Type type;

    public static KVPair delete(byte[] rowKey) {
        return new KVPair(rowKey, HConstants.EMPTY_BYTE_ARRAY,Type.DELETE);
    }

    public long getSize() {
        return rowKey.length+value.length;
    }

		public void toBytes(Output output) {
				output.writeByte(type.asByte());
				output.writeInt(rowKey.length);
				output.write(rowKey);
				output.writeInt(value.length);
				output.write(value);
		}

		public static KVPair fromBytes(Input input){
				Type type = Type.decode(input.readByte());
				byte[] rowKey = new byte[input.readInt()];
				input.read(rowKey);
				byte[] value = new byte[input.readInt()];
				input.read(value);
				return new KVPair(rowKey,value,type);
		}


		public enum Type{
        INSERT((byte)0x01),
        UPDATE((byte)0x02),
        DELETE((byte)0x03),
				EMPTY_COLUMN((byte)0x04);

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

    public KVPair(){
        this.type = Type.INSERT;
    }

    public KVPair(byte[] rowKey, byte[] value) {
        this(rowKey, value,Type.INSERT);
    }

    public KVPair(byte[] rowKey, byte[] value, Type writeType){
        this.rowKey = rowKey;
        this.value = value;
        this.type = writeType;
    }


    public byte[] getValue(){
        return value;
    }

    public byte[] getRow(){
        return rowKey;
    }

    public Type getType(){
        return type;
    }

    public void setValue(byte[] value){
        this.value = value;
    }

    public void setKey(byte[] key){
        this.rowKey = key;
    }

    public Put toPut(byte[] cf, byte[] qualifier,long timestamp){
        Put put = new Put(rowKey);
        put.add(cf,qualifier,timestamp,value);
        return put;
    }

    public KeyValue toKeyValue(){
        return new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,value);
    }

    @Override
    public int compareTo(KVPair o) {
        if(o==null) return 1;
        return Bytes.compareTo(rowKey,o.rowKey);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rowKey.length);
        out.write(rowKey);
        out.writeInt(value.length);
        out.write(value);
        out.writeUTF(type.name());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowKey = new byte[in.readInt()];
        in.readFully(rowKey);
        value = new byte[in.readInt()];
        in.readFully(value);
        type = Type.valueOf(in.readUTF());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KVPair)) return false;

        KVPair kvPair = (KVPair) o;

        return type == kvPair.type && Bytes.equals(rowKey,kvPair.rowKey);

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rowKey);
        result = 31 * result + type.hashCode();
        return result;
    }
}
