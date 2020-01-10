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

package com.splicemachine.olap;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
@ChannelHandler.Sharable
public class OlapStatusHandler extends AbstractOlapHandler{
    private static final Logger LOG = Logger.getLogger(OlapStatusHandler.class);

    public OlapStatusHandler(OlapJobRegistry registry){
        super(registry);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, OlapMessage.Command cmd) throws Exception {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Received " + cmd);
        }
        if(cmd.getType()!=OlapMessage.Command.Type.STATUS){
            ctx.fireChannelRead(cmd);
            return;
        }
        OlapJobStatus status = jobRegistry.getStatus(cmd.getUniqueName());

        OlapMessage.Status statusMsg = cmd.getExtension(OlapMessage.Status.command);
        if (statusMsg.hasWaitTimeMillis() && status != null) {
            long waitTime = statusMsg.getWaitTimeMillis();
            switch (status.checkState()) {
                case SUBMITTED:
                case RUNNING:
                    status.wait(waitTime, TimeUnit.MILLISECONDS);
                default:
                    // fall-through, send response without blocking
            }
        }


        writeResponse(ctx.channel(),cmd.getUniqueName(),status);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Status " + status);
        }

    }
}
