package com.splicemachine.olap;

import com.splicemachine.olap.OlapMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.sparkproject.jboss.netty.channel.*;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class OlapStatusHandler extends AbstractOlapHandler{

    public OlapStatusHandler(OlapJobRegistry registry){
        super(registry);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx,MessageEvent e) throws Exception{
        OlapMessage.Command cmd = (OlapMessage.Command)e.getMessage();
        if(cmd.getType()!=OlapMessage.Command.Type.STATUS){
            ctx.sendUpstream(e);
            return;
        }
        OlapJobStatus status = jobRegistry.getStatus(cmd.getUniqueName());
        writeResponse(e,cmd.getUniqueName(),status);

        super.messageReceived(ctx,e);
    }
}
