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
import org.jboss.netty.buffer.ChannelBuffer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 7/15/14
 */
public class AsyncAttributeHolder extends ScanFilter {
    private static final byte[] NAME = Bytes.ISO88591(HbaseAttributeHolder.class.getName());
    private final Map<String,byte[]> attributes;

    public AsyncAttributeHolder(Map<String, byte[]> attributes) {
        this.attributes = attributes;
    }

    @Override public byte[] name() { return NAME; }

    @Override
    public byte[] serialize() {
        SpliceMessage.HbaseAttributeHolderMessage.Builder filterM = SpliceMessage.HbaseAttributeHolderMessage.newBuilder();
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            filterM.addAttributes(SpliceMessage.HbaseAttributeHolderMessage.Attribute.newBuilder().setName(entry.getKey())
                    .setValue(ZeroCopyLiteralByteString.wrap(entry.getValue())).build());
        }
        return filterM.build().toByteArray();
    }

    public static HbaseAttributeHolder parseFrom(byte[] pbBytes) throws DeserializationException {
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

    @Override
    public void serializeOld(ChannelBuffer buf) {
        buf.writeByte((byte)NAME.length);
        buf.writeBytes(NAME);

        HBaseRpc.writeVLong(buf,attributes.size());
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            byte[] keyStr = entry.getKey().getBytes();
            HBaseRpc.writeByteArray(buf, keyStr);
            byte[] val = entry.getValue();
            HBaseRpc.writeByteArray(buf,val);
        }
    }

    @Override
    public int predictSerializedSize() {
        int size = 1+NAME.length+1;
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            size+=entry.getKey().length()+1+entry.getValue().length+4;
        }
        return size;
    }
}
