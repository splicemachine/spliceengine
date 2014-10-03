package com.splicemachine.async;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 7/15/14
 */
public class AsyncAttributeHolder extends ScanFilter {
    private static final byte[] NAME = Bytes.ISO88591(com.splicemachine.async.HbaseAttributeHolder.class.getName());
    private final Map<String,byte[]> attributes;

    public AsyncAttributeHolder(Map<String, byte[]> attributes) {
        this.attributes = attributes;
    }

    @Override byte[] name() { return NAME; }

    @Override
    byte[] serialize() {
        throw new UnsupportedOperationException("IMPLEMENT PROTOBUFS FOR 0.96");
    }

    @Override
    void serializeOld(ChannelBuffer buf) {
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
    int predictSerializedSize() {
        int size = 1+NAME.length+1;
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            size+=entry.getKey().length()+1+entry.getValue().length+4;
        }
        return size;
    }
}
