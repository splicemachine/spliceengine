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

package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.channel.*;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public abstract class AbstractOlapHandler extends SimpleChannelUpstreamHandler{

    protected final OlapJobRegistry jobRegistry;

    public AbstractOlapHandler(OlapJobRegistry jobRegistry){
        this.jobRegistry=jobRegistry;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,ExceptionEvent e) throws Exception{
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
