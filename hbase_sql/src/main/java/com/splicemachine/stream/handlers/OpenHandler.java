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

import com.splicemachine.stream.KryoDecoder;
import com.splicemachine.stream.KryoEncoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * We are using Kryo for serialization message between spark and the Olap Server.
 * This handler is going to simply take the current pipeline and add 2 standard channels
 * to encode a kryo object and decode a kryo object.
 * Also it is going to add the main channel handler in the pipleline
 * @see com.splicemachine.stream.StreamListenerServer
 * @see com.splicemachine.stream.ResultStreamer
 */

@ChannelHandler.Sharable
public class OpenHandler extends ChannelInitializer<SocketChannel> {

    private ChannelInboundHandlerAdapter listener;

    public OpenHandler(ChannelInboundHandlerAdapter listener) {
        this.listener = listener;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        ch.pipeline().addLast(
                new KryoEncoder(),
                new KryoDecoder(),
                listener);
    }

}