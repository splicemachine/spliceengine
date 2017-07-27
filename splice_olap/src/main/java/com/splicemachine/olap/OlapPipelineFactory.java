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

package com.splicemachine.olap;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.olap.OlapMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

public class OlapPipelineFactory implements ChannelPipelineFactory {

    private static final Logger LOG = Logger.getLogger(OlapPipelineFactory.class);

    private final ChannelHandler submitHandler;
    private final ChannelHandler cancelHandler;
    private final ChannelHandler statusHandler;

    private final ProtobufDecoder decoder;

    public OlapPipelineFactory(ChannelHandler submitHandler,ChannelHandler cancelHandler,ChannelHandler statusHandler){
        this.submitHandler=submitHandler;
        this.cancelHandler=cancelHandler;
        this.statusHandler=statusHandler;

        this.decoder = new ProtobufDecoder(OlapMessage.Command.getDefaultInstance(),buildExtensionRegistry());
    }


    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SpliceLogUtils.trace(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("frameDecoder",new LengthFieldBasedFrameDecoder(1<<30,0,4,0,4)); //max frame size is 1GB=2^30 bytes
        pipeline.addLast("protobufDecoder",decoder);
        pipeline.addLast("frameEncoder",new LengthFieldPrepender(4));
        pipeline.addLast("protobufEncoder",new ProtobufEncoder());
        pipeline.addLast("statusHandler", statusHandler);
        pipeline.addLast("submitHandler", submitHandler);
        pipeline.addLast("cancelHandler",cancelHandler);
        SpliceLogUtils.trace(LOG, "Done creating channel pipeline");
        return pipeline;
    }


    private ExtensionRegistry buildExtensionRegistry(){
        ExtensionRegistry er = ExtensionRegistry.newInstance();
        er.add(OlapMessage.Submit.command);
        er.add(OlapMessage.Status.command);
        er.add(OlapMessage.Cancel.command);
        return er;
    }
}
