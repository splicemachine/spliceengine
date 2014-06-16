package com.splicemachine.si.impl.timestamp;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;

public class TimestampPipelineFactoryLite implements ChannelPipelineFactory {

    private static final Logger LOG = Logger.getLogger(TimestampPipelineFactoryLite.class);

	private ChannelHandler _tsHandler = null;

	public TimestampPipelineFactoryLite(ChannelHandler handler) {
	    _tsHandler = handler;
	}

	public ChannelPipeline getPipeline() throws Exception {
	    TimestampUtil.doServerDebug(LOG, "getPipeline: creating new pipeline (lite)");
        ChannelPipeline pipeline = Channels.pipeline();
        ((TimestampServerHandler)_tsHandler).initializeIfNeeded();
		pipeline.addLast("decoder", new FixedLengthFrameDecoder(4)); // We receive 4 byte id from client
        pipeline.addLast("handler", _tsHandler);
        return pipeline;
    }
    
}
