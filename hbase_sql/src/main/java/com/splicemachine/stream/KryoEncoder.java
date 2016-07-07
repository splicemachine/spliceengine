package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.sparkproject.io.netty.buffer.ByteBuf;
import org.sparkproject.io.netty.channel.ChannelHandlerContext;
import org.sparkproject.io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;

public class KryoEncoder extends MessageToByteEncoder<Object> {

    private final Kryo kryo;
    ByteArrayOutputStream outStream;
    Output output;

    public KryoEncoder(Kryo kryo) {
        this.kryo = kryo;
        outStream = new ByteArrayOutputStream();
        output = new Output(outStream, 4096);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
        outStream.reset();

        kryo.writeClassAndObject(output, in);
        output.flush();

        byte[] outArray = outStream.toByteArray();
        out.writeShort(outArray.length);
        out.writeBytes(outArray);
    }

}
