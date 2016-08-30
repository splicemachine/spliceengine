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
import com.esotericsoftware.kryo.io.Input;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.utils.kryo.KryoPool;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.log4j.Logger;
import java.util.List;

public class KryoDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = Logger.getLogger(KryoDecoder.class);
    static private KryoPool kp = SpliceSparkKryoRegistrator.getInstance();

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

        Kryo decoder = kp.get();
        try {
            Object object = decoder.readClassAndObject(input);
            out.add(object);
        }
        finally {
            kp.returnInstance(decoder);

        }


//        LOG.warn("Decoded " + object);
    }
}
