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
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

@ChannelHandler.Sharable
public class TimestampServerHandler extends TimestampBaseHandler<TimestampMessage.TimestampRequest> {

    private static final Logger LOG = Logger.getLogger(TimestampServerHandler.class);

    private volatile TimestampOracle oracle;

    public TimestampServerHandler(TimestampOracle oracle) {
        super();
        this.oracle = oracle;
    }

    @Override
    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION", justification = "We fail the RPC with the exception")
    protected void channelRead0(ChannelHandlerContext ctx, TimestampMessage.TimestampRequest request) throws Exception {
        TimestampMessage.TimestampResponse.Builder responseBuilder = TimestampMessage.TimestampResponse.newBuilder()
                .setCallerId(request.getCallerId())
                .setTimestampRequestType(request.getTimestampRequestType());
        try {
            assert oracle != null;

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
                case GET_TIMESTAMP_BATCH:
                    int batchSize = request.getTimestampBatch().getBatchSize();
                    Pair<Long, Integer> result = oracle.getTimestampBatch(batchSize);
                    long firstTimestamp = result.getFirst();
                    int delta = result.getSecond();
                    assert firstTimestamp > 0;
                    responseBuilder.setTimestampBatchResponse(
                            TimestampMessage.GetTimestampBatchResponse.newBuilder()
                                    .setBatchSize(batchSize)
                                    .setTimestampDelta(delta)
                                    .setFirstTimestamp(firstTimestamp));
                    break;
                default:
                    assert false;
            }
            //
            // Respond to the client
            //

            TimestampMessage.TimestampResponse response = responseBuilder.build();

            SpliceLogUtils.debug(LOG, "Responding to caller %s with response %s", response.getCallerId(), response);
            ChannelFuture futureResponse = ctx.channel().writeAndFlush(response); // Could also use Channels.write
            futureResponse.addListener(cf -> {
                        if (!cf.isSuccess()) {
                            throw new TimestampIOException(
                                    "Failed to respond successfully to caller id " + response.getCallerId(), cf.cause());
                        }
                    }
            );
        } catch (Exception e) {
            ctx.writeAndFlush(responseBuilder.setErrorMessage(e.toString()).build());
        }
    }

    protected void doError(String message, Throwable t, Object... args) {
        SpliceLogUtils.error(LOG, message, t, args);
    }

}
