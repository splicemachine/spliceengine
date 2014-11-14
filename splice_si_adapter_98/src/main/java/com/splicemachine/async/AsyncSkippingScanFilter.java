package com.splicemachine.async;

import org.apache.hadoop.hbase.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import com.splicemachine.hbase.AbstractSkippingScanFilter;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public class AsyncSkippingScanFilter extends ScanFilter {
    private static final byte[] NAME = Bytes.ISO88591(AsyncSkippingScanFilter.class.getName());
    private List<Pair<byte[],byte[]>> startStopKeys;
    private List<byte[]> predicates;

    public AsyncSkippingScanFilter(AbstractSkippingScanFilter sf){
        this.startStopKeys = sf.getStartStopKeys();
        this.predicates = sf.getPredicates();
    }

    @Override byte[] name() { return NAME; }

    @Override
    byte[] serialize() {
        throw new UnsupportedOperationException("IMPLEMENT FOR 0.96+");
    }

    @Override
    void serializeOld(ChannelBuffer buf) {
        buf.writeByte((byte)NAME.length);
        buf.writeBytes(NAME);
        buf.writeInt(startStopKeys.size());
        for(int i=0;i<startStopKeys.size();i++){
            Pair<byte[],byte[]> startStop = startStopKeys.get(i);
            byte[] start =startStop.getFirst();
            buf.writeInt(start.length);
            buf.writeBytes(start);
            byte[] stop = startStop.getSecond();
            buf.writeInt(stop.length);
            buf.writeBytes(stop);

            byte[] pred = predicates.get(i);
            buf.writeInt(pred.length);
            buf.writeBytes(pred);
        }
    }

    @Override
    int predictSerializedSize() {
        int size = 5+NAME.length;
        for(int i=0;i<startStopKeys.size();i++){
            Pair<byte[], byte[]> pair = startStopKeys.get(i);
            size+=4+ pair.getFirst().length+4+pair.getSecond().length;
            size+=predicates.get(i).length+4;
        }
        return size;
    }
}
