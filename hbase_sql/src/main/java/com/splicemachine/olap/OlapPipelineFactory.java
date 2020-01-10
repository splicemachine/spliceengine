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

package com.splicemachine.olap;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.utils.SpliceLogUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.log4j.Logger;

public class OlapPipelineFactory extends ChannelInitializer {

    private static final Logger LOG = Logger.getLogger(OlapPipelineFactory.class);

    private final ChannelInboundHandler submitHandler;
    private final ChannelInboundHandler cancelHandler;
    private final ChannelInboundHandler statusHandler;

    private final ProtobufDecoder decoder;

    public OlapPipelineFactory(ChannelInboundHandler submitHandler, ChannelInboundHandler cancelHandler, ChannelInboundHandler statusHandler){
        this.submitHandler=submitHandler;
        this.cancelHandler=cancelHandler;
        this.statusHandler=statusHandler;

        this.decoder = new ProtobufDecoder(OlapMessage.Command.getDefaultInstance(),buildExtensionRegistry());
    }

    private ExtensionRegistry buildExtensionRegistry(){
        ExtensionRegistry er = ExtensionRegistry.newInstance();
        er.add(OlapMessage.Submit.command);
        er.add(OlapMessage.Status.command);
        er.add(OlapMessage.Cancel.command);
        return er;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        SpliceLogUtils.trace(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("frameDecoder",new LengthFieldBasedFrameDecoder(1<<30,0,4,0,4)); //max frame size is 1GB=2^30 bytes
        pipeline.addLast("protobufDecoder",decoder);
        pipeline.addLast("frameEncoder",new LengthFieldPrepender(4));
        pipeline.addLast("protobufEncoder",new ProtobufEncoder());
        pipeline.addLast("statusHandler", statusHandler);
        pipeline.addLast("submitHandler", submitHandler);
        pipeline.addLast("cancelHandler",cancelHandler);
        SpliceLogUtils.trace(LOG, "Done creating channel pipeline");
    }
}
