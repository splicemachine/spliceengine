package com.splicemachine.si.impl.timestamp;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;

public class TimestampPipelineFactoryLite implements ChannelPipelineFactory {

	// This pipeline factory is called "lite" to distinguish it from
	// more sophisticated implementations that might do things like
	// specify an Executor.
	
    private static final Logger LOG = Logger.getLogger(TimestampPipelineFactoryLite.class);

	private ChannelHandler _tsHandler = null;

	public TimestampPipelineFactoryLite(ChannelHandler handler) {
	    _tsHandler = handler;
	}

	public ChannelPipeline getPipeline() throws Exception {
	    TimestampUtil.doServerDebug(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = Channels.pipeline();
        ((TimestampServerHandler)_tsHandler).initializeIfNeeded();
		pipeline.addLast("decoder", new FixedLengthFrameDecoder(TimestampServer.FIXED_MSG_RECEIVED_LENGTH));
        pipeline.addLast("handler", _tsHandler);
	    TimestampUtil.doServerDebug(LOG, "Done creating channel pipeline");
        return pipeline;
    }
    
}
