/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
