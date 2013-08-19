package com.splicemachine.hbase.writer;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
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
        return new KVPair(rowKey, RowEncoder.EMPTY_BYTES,Type.DELETE);
    }

    public long getSize() {
        return rowKey.length+value.length;
    }

    public enum Type{
        INSERT,
        UPDATE,
        DELETE
    }

    public KVPair(){}

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

    public Put toPut(){
        Put put = new Put(rowKey);
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY,value);
        return put;
    }

    public KeyValue toKeyValue(){
        return new KeyValue(rowKey,SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,value);
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

        return Arrays.equals(rowKey, kvPair.rowKey) && type == kvPair.type;

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rowKey);
        result = 31 * result + type.hashCode();
        return result;
    }
}
