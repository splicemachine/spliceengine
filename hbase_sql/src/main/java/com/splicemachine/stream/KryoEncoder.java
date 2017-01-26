/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.utils.kryo.KryoPool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;

public class KryoEncoder extends MessageToByteEncoder<Object> {


    ByteArrayOutputStream outStream;
    Output output;


    static private KryoPool kp = SpliceSparkKryoRegistrator.getInstance();


    public KryoEncoder() {
        outStream = new ByteArrayOutputStream();
        output = new Output(outStream, 4096);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object in, ByteBuf out) throws Exception {
        outStream.reset();

        Kryo encoder = kp.get();
        try {
            encoder.writeClassAndObject(output, in);
        } finally {
            kp.returnInstance(encoder);
        }

        output.flush();
        byte[] outArray = outStream.toByteArray();
        out.writeShort(outArray.length);
        out.writeBytes(outArray);
    }

}
