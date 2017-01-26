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
