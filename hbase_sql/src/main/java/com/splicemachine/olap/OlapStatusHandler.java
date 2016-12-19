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

import com.splicemachine.olap.OlapMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.channel.*;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class OlapStatusHandler extends AbstractOlapHandler{
    private static final Logger LOG = Logger.getLogger(OlapStatusHandler.class);

    public OlapStatusHandler(OlapJobRegistry registry){
        super(registry);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx,MessageEvent e) throws Exception{
        OlapMessage.Command cmd = (OlapMessage.Command)e.getMessage();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Received " + cmd);
        }
        if(cmd.getType()!=OlapMessage.Command.Type.STATUS){
            ctx.sendUpstream(e);
            return;
        }
        OlapJobStatus status = jobRegistry.getStatus(cmd.getUniqueName());
        writeResponse(e,cmd.getUniqueName(),status);

        if (LOG.isTraceEnabled()) {
            LOG.trace("Status " + status);
        }

        super.messageReceived(ctx,e);
    }
}
