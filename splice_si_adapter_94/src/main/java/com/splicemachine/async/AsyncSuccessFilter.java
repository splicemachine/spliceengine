package com.splicemachine.async;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.List;

/**
 * @author Scott Fines
 * Date: 7/16/14
 */
public class AsyncSuccessFilter extends ScanFilter {
    private static final byte[] NAME = Bytes.ISO88591("com.splicemachine.derby.impl.job.operation.SuccessFilter");
    private List<byte[]> failedTasks;

    public AsyncSuccessFilter(List<byte[]> failedTasks) {
        this.failedTasks = failedTasks;
    }

    @Override public byte[] name() { return NAME; }

    @Override
    public byte[] serialize() {
        throw new UnsupportedOperationException("IMPLEMENT FOR 0.96+");
    }

    @Override
    public void serializeOld(ChannelBuffer buf) {
        buf.writeByte((byte)NAME.length);
        buf.writeBytes(NAME);

        buf.writeInt(failedTasks.size());
        for (byte[] n : failedTasks) {
            buf.writeInt(n.length);
            buf.writeBytes(n);
        }
    }

    @Override
    public int predictSerializedSize() {
        int size = 5+NAME.length;
        for(byte[] n :failedTasks){
            size+=n.length+4;
        }
        return size;
    }
}
