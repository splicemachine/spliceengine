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

package com.splicemachine.timestamp.impl;

import com.splicemachine.utils.SpliceLogUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.log4j.Logger;

public class TimestampPipelineFactoryLite extends ChannelInitializer {

    // This pipeline factory is called "lite" to distinguish it from
    // more sophisticated implementations that might do things like
    // specify an Executor.
    
    private static final Logger LOG = Logger.getLogger(TimestampPipelineFactoryLite.class);

    private ChannelHandler tsHandler = null;

    public TimestampPipelineFactoryLite(ChannelHandler handler) {
        tsHandler = handler;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        SpliceLogUtils.debug(LOG, "Creating new channel pipeline...");
        ch.pipeline()
                .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                .addLast("protobufDecoder", new ProtobufDecoder(TimestampMessage.TimestampRequest.getDefaultInstance()))
                .addLast("frameEncoder", new LengthFieldPrepender(4))
                .addLast("protobufEncoder", new ProtobufEncoder())
                .addLast("handler", tsHandler);
        SpliceLogUtils.debug(LOG, "Done creating channel pipeline");
    }
}
