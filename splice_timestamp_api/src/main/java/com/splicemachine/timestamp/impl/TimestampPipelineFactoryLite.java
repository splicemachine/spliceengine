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

package com.splicemachine.timestamp.impl;

import com.splicemachine.utils.SpliceLogUtils;
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
