package com.splicemachine.hbase.writer;

import com.splicemachine.constants.SpliceConstants;
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
    private static final long serialVersionUID = 1l;
    private byte[] rowKey;
    private byte[] value;

    public KVPair(byte[] rowKey, byte[] value) {
        this.rowKey = rowKey;
        this.value = value;
    }

    public byte[] getValue(){
        return value;
    }

    public byte[] getRow(){
        return rowKey;
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
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowKey = new byte[in.readInt()];
        in.read(rowKey);
        value = new byte[in.readInt()];
        in.read(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KVPair)) return false;

        KVPair kvPair = (KVPair) o;

        return Bytes.equals(rowKey,kvPair.rowKey);
    }

    @Override
    public int hashCode() {
        return rowKey != null ? Arrays.hashCode(rowKey) : 0;
    }
}
