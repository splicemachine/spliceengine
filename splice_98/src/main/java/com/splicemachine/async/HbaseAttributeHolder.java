package com.splicemachine.async;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.WritableUtils;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 7/15/14
 */
public class HbaseAttributeHolder extends FilterBase {

    public HbaseAttributeHolder() { }

    private Map<String,byte[]> attributes;

    public HbaseAttributeHolder(Map<String, byte[]> attrMap) {
        this.attributes = attrMap;
    }

    public Map<String, byte[]> getAttributes() { return attributes; }


    @Override
    public byte[] toByteArray() throws IOException {
        SpliceMessage.HbaseAttributeHolderMessage.Builder filterM = SpliceMessage.HbaseAttributeHolderMessage.newBuilder();
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            filterM.addAttributes(SpliceMessage.HbaseAttributeHolderMessage.Attribute.newBuilder().setName(entry.getKey())
                    .setValue(ZeroCopyLiteralByteString.wrap(entry.getValue())).build());
        }
        return filterM.build().toByteArray();
    }

    public static HbaseAttributeHolder parseFrom(byte[] pbBytes) throws DeserializationException{
        SpliceMessage.HbaseAttributeHolderMessage proto;
        try{
            proto = SpliceMessage.HbaseAttributeHolderMessage.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        Map<String,byte[]> attributes = Maps.newHashMap();

        int s = proto.getAttributesCount();
        for(int i=0;i<s;i++){
            SpliceMessage.HbaseAttributeHolderMessage.Attribute attr = proto.getAttributes(i);
            attributes.put(attr.getName(),attr.getValue().toByteArray());
        }
        return new HbaseAttributeHolder(attributes);
    }

//    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out,attributes.size());
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            org.apache.hadoop.hbase.util.Bytes.writeByteArray(out,entry.getKey().getBytes());
            org.apache.hadoop.hbase.util.Bytes.writeByteArray(out,entry.getValue());
        }
    }

//    @Override
    public void readFields(DataInput in) throws IOException {
        int size = (int)WritableUtils.readVLong(in);
        attributes = Maps.newHashMap();
        for(int i=0;i<size;i++){
            byte[] keyBytes = org.apache.hadoop.hbase.util.Bytes.readByteArray(in);
            byte[] valueBytes = org.apache.hadoop.hbase.util.Bytes.readByteArray(in);
            attributes.put(new String(keyBytes),valueBytes);
        }
    }
}
