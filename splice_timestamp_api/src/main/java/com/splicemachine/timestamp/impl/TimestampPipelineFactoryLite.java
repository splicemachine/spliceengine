package com.splicemachine.timestamp.impl;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.channel.ChannelHandler;
import org.sparkproject.jboss.netty.channel.ChannelPipeline;
import org.sparkproject.jboss.netty.channel.ChannelPipelineFactory;
import org.sparkproject.jboss.netty.channel.Channels;
import org.sparkproject.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;

public class TimestampPipelineFactoryLite implements ChannelPipelineFactory {

    // This pipeline factory is called "lite" to distinguish it from
    // more sophisticated implementations that might do things like
    // specify an Executor.
    
    private static final Logger LOG = Logger.getLogger(TimestampPipelineFactoryLite.class);

    private ChannelHandler tsHandler = null;

    public TimestampPipelineFactoryLite(ChannelHandler handler) {
        tsHandler = handler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SpliceLogUtils.debug(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = Channels.pipeline();
        ((TimestampServerHandler) tsHandler).initializeIfNeeded();
        pipeline.addLast("decoder", new FixedLengthFrameDecoder(TimestampServer.FIXED_MSG_RECEIVED_LENGTH));
        pipeline.addLast("handler", tsHandler);
        SpliceLogUtils.debug(LOG, "Done creating channel pipeline");
        return pipeline;
    }
    
}
