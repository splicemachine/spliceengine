package com.splicemachine.olap;

import com.splicemachine.olap.OlapMessage;
import org.sparkproject.jboss.netty.channel.ChannelHandlerContext;
import org.sparkproject.jboss.netty.channel.MessageEvent;
import org.sparkproject.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class OlapCancelHandler extends AbstractOlapHandler{


    public OlapCancelHandler(OlapJobRegistry registry){
        super(registry);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx,MessageEvent e) throws Exception{
        OlapMessage.Command cmd=(OlapMessage.Command)e.getMessage();
        if(cmd.getType()!=OlapMessage.Command.Type.CANCEL){
            ctx.sendUpstream(e);
            return;
        }

        jobRegistry.clear(cmd.getUniqueName());
        //no response is needed for cancellation
    }
}
