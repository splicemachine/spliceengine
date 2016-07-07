package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.log4j.Logger;
import org.sparkproject.io.netty.buffer.ByteBuf;
import org.sparkproject.io.netty.channel.ChannelHandlerContext;
import org.sparkproject.io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class KryoDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = Logger.getLogger(KryoDecoder.class);
    
    private final Kryo kryo;

    public KryoDecoder(Kryo kryo) {
        this.kryo = kryo;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
//        LOG.warn("Decoding");
        
        if (in.readableBytes() < 2)
            return;


        in.markReaderIndex();

        int len = in.readUnsignedShort();
//        LOG.warn("Read lenght " + len);

        if (in.readableBytes() < len) {

//            LOG.warn("Not enough data ");
            in.resetReaderIndex();
            return;
        }

//        LOG.warn("Decoding object ");

        byte[] buf = new byte[len];
        in.readBytes(buf);
        Input input = new Input(buf);
        Object object = kryo.readClassAndObject(input);
        out.add(object);

//        LOG.warn("Decoded " + object);
    }
}
