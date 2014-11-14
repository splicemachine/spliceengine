package org.hbase.async;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 * Date: 7/15/14
 */
public class HbaseAttributeHolder extends FilterBase {

    public HbaseAttributeHolder() { }

    public HbaseAttributeHolder(Map<String,byte[]> attributes) {
        this.attributes = attributes;
    }

    private Map<String,byte[]> attributes;

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