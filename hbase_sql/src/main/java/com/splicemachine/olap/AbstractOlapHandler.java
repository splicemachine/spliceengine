/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.*;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public abstract class AbstractOlapHandler extends SimpleChannelUpstreamHandler {

    protected final OlapJobRegistry jobRegistry;

    public AbstractOlapHandler(OlapJobRegistry jobRegistry){
        this.jobRegistry=jobRegistry;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception{
        Logger.getLogger(this.getClass()).warn("Unexpected error caught in Olap pipeline: ",e.getCause());
        final Throwable t = e.getCause();
        Channel c = e.getChannel();
        ChannelFuture futureResponse = c.write(OlapSerializationUtils.buildError(t));
        futureResponse.addListener(new ChannelFutureListener(){
            @Override
            public void operationComplete(ChannelFuture future) throws Exception{
                if(shouldDisconnect(t))
                    future.getChannel().close();
            }
        });
    }

    /* ****************************************************************************************************************/
    /*Protected convenience methods*/
    protected void writeResponse(MessageEvent e,final String requestId,OlapStatus status) throws IOException{
        Channel c = e.getChannel();
        final boolean[] shouldRemove=new boolean[]{false};
        ChannelFuture futureResponse = c.write(OlapSerializationUtils.buildResponse(status,shouldRemove,jobRegistry.tickTime()));

        futureResponse.addListener(new ChannelFutureListener(){
                                       @Override
                                       public void operationComplete(ChannelFuture cf) throws Exception{
                                           if(!cf.isSuccess()){
                                               throw new IOException(
                                                       "Failed to respond successfully to message "+requestId,
                                                       cf.getCause());
                                           }else if (shouldRemove[0]){
                                               jobRegistry.clear(requestId);
                                           }

                                       }
                                   }
        );
    }

    protected boolean shouldDisconnect(Throwable t){
        /*
         * This is a placeholder for errors which should cause us to forcibly disconnect a channel due to
         * an unexpected error(like OutOfMemory, etc).
         */
        return t instanceof Error;
    }
}
