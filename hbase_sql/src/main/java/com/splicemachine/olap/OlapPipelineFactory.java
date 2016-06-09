package com.splicemachine.olap;

import com.google.protobuf.ExtensionRegistry;
import com.splicemachine.olap.OlapMessage;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.channel.ChannelHandler;
import org.sparkproject.jboss.netty.channel.ChannelPipeline;
import org.sparkproject.jboss.netty.channel.ChannelPipelineFactory;
import org.sparkproject.jboss.netty.channel.Channels;
import org.sparkproject.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.sparkproject.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.sparkproject.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.sparkproject.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

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
        pipeline.addLast("frameDecoder",new LengthFieldBasedFrameDecoder(1<<20,0,4,0,4)); //max frame size is 1MB=2^20 bytes
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
