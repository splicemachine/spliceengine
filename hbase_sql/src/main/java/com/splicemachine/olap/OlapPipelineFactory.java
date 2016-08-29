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
