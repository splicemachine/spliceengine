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

package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

public class TimestampServerHandler extends TimestampBaseHandler {

    private static final Logger LOG = Logger.getLogger(TimestampServerHandler.class);

    private volatile TimestampOracle oracle;
    private TimestampBlockManager timestampBlockManager;
    private int blockSize;

    public TimestampServerHandler(TimestampBlockManager timestampBlockManager, int blockSize) {
        super();
        this.timestampBlockManager=timestampBlockManager;
        this.blockSize = blockSize;
    }

    public void initializeIfNeeded() throws TimestampIOException{
        SpliceLogUtils.trace(LOG, "Checking whether initialization is needed");
        if(oracle==null){
            synchronized(this){
                if(oracle==null){
                    oracle=TimestampOracle.getInstance(timestampBlockManager,blockSize);
                }
            }
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        assert oracle != null;
        TimestampMessage.TimestampRequest request = (TimestampMessage.TimestampRequest) e.getMessage();
        TimestampMessage.TimestampResponse.Builder responseBuilder = TimestampMessage.TimestampResponse.newBuilder()
                .setCallerId(request.getCallerId())
                .setTimestampRequestType(request.getTimestampRequestType());

        SpliceLogUtils.trace(LOG, "Received timestamp request from client. Caller id = %s", request.getCallerId());
        switch (request.getTimestampRequestType()) {
            case GET_CURRENT_TIMESTAMP: {
                long timestamp = oracle.getCurrentTimestamp();
                assert timestamp > 0;
                responseBuilder.setGetCurrentTimestampResponse(
                        TimestampMessage.GetCurrentTimestampResponse.newBuilder().setTimestamp(timestamp));
                break;
            }
            case GET_NEXT_TIMESTAMP: {
                long timestamp = oracle.getNextTimestamp();
                assert timestamp > 0;
                responseBuilder.setGetNextTimestampResponse(
                        TimestampMessage.GetNextTimestampResponse.newBuilder().setTimestamp(timestamp));
                break;
            }
            case BUMP_TIMESTAMP:
                oracle.bumpTimestamp(request.getBumpTimestamp().getTimestamp());
                break;
            default:
                assert false;
        }
        //
        // Respond to the client
        //

        TimestampMessage.TimestampResponse response = responseBuilder.build();

        SpliceLogUtils.debug(LOG, "Responding to caller %s with response %s", response.getCallerId(), response);
        ChannelFuture futureResponse = e.getChannel().write(response); // Could also use Channels.write
        futureResponse.addListener(cf -> {
            if (!cf.isSuccess()) {
                throw new TimestampIOException(
                        "Failed to respond successfully to caller id " + response.getCallerId(), cf.getCause());
            }
        }
        );

        super.messageReceived(ctx, e);
    }

    protected void doError(String message, Throwable t, Object... args) {
        SpliceLogUtils.error(LOG, message, t, args);
    }

}
