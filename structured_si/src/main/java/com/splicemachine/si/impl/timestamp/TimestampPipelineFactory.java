package com.splicemachine.si.impl.timestamp;

import java.util.concurrent.Executor;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;

public class TimestampPipelineFactory implements ChannelPipelineFactory {

    private static final Logger LOG = Logger.getLogger(TimestampPipelineFactory.class);

    private Executor _pipelineExecutor = null;

	private ExecutionHandler _executionHandler = null;
	private ChannelHandler _tsHandler = null;

	public TimestampPipelineFactory(Executor pipelineExecutor, ChannelHandler handler) {
	    _pipelineExecutor = pipelineExecutor;
	    _tsHandler = handler;
	}

	public ChannelPipeline getPipeline() throws Exception {
	    TimestampUtil.doServerDebug(LOG, "getPipeline: creating new pipeline");
        ChannelPipeline pipeline = Channels.pipeline();
       
        if (_executionHandler == null) {
	        synchronized(this) {
	            if (_executionHandler == null) { // check again
	         	   _executionHandler = new ExecutionHandler(_pipelineExecutor);
	            }
	        }
        }
       
        ((TimestampServerHandler)_tsHandler).initializeIfNeeded();

        pipeline.addLast("executor", _executionHandler);
        pipeline.addLast("handler", _tsHandler);

        return pipeline;
    }

}
