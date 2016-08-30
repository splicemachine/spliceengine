/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
