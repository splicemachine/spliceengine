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

package com.splicemachine.stream.handlers;

import com.esotericsoftware.kryo.Kryo;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.stream.KryoDecoder;
import com.splicemachine.stream.KryoEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.sparkproject.io.netty.channel.ChannelHandler;
import org.sparkproject.io.netty.channel.ChannelHandlerContext;
import org.sparkproject.io.netty.channel.ChannelInboundHandlerAdapter;
import org.sparkproject.io.netty.channel.ChannelInitializer;
import org.sparkproject.io.netty.channel.socket.SocketChannel;


@ChannelHandler.Sharable
public class OpenHandler extends ChannelInitializer<SocketChannel> {

    Kryo encoder;
    Kryo decoder;
    KryoPool kp = SpliceSparkKryoRegistrator.getInstance();
    ChannelInboundHandlerAdapter listener;

    public OpenHandler(ChannelInboundHandlerAdapter listener) {
        this.listener = listener;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        // need to keep reference so we can return it to the pool
        encoder = kp.get();
        decoder = kp.get();

        ch.pipeline().addLast(
                new KryoEncoder(encoder),
                new KryoDecoder(decoder),
                listener);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)  throws Exception {
        //when the channel disconnect release the kryo to the pool
        kp.returnInstance(encoder);
        kp.returnInstance(decoder);

    }

}