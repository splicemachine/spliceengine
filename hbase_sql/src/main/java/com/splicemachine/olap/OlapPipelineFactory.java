package com.splicemachine.olap;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.MemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OlapPipelineFactory implements ChannelPipelineFactory {

    private static final Logger LOG = Logger.getLogger(OlapPipelineFactory.class);

    private ChannelHandler olapHandler = null;
    private ExecutionHandler executionHandler;

    public OlapPipelineFactory(ChannelHandler handler) {
        olapHandler = handler;
        executionHandler = new ExecutionHandler(
                new MemoryAwareThreadPoolExecutor(16, 1048576, 1048576, 30, TimeUnit.SECONDS,
                        Executors.defaultThreadFactory()));
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SpliceLogUtils.debug(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)));
        pipeline.addLast("encoder", new ObjectEncoder());
        pipeline.addLast("executor", executionHandler);
        pipeline.addLast("handler", olapHandler);
        SpliceLogUtils.debug(LOG, "Done creating channel pipeline");
        return pipeline;
    }
    
}
