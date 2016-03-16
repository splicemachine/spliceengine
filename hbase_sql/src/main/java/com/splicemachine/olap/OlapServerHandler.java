package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapCallable;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.*;

import java.io.IOException;

public class OlapServerHandler extends SimpleChannelUpstreamHandler {

    private static final Logger LOG = Logger.getLogger(OlapServerHandler.class);

    public OlapServerHandler() {
        super();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        final OlapCallable<?> callable = (OlapCallable<?>) e.getMessage();
        assert callable != null;

        OlapResult result = callable.call();
        result.setCallerId(callable.getCallerId());

        //
        // Respond to the client
        //

        ChannelFuture futureResponse = e.getChannel().write(result);
        futureResponse.addListener(new ChannelFutureListener() {
                                       @Override
                                       public void operationComplete(ChannelFuture cf) throws Exception {
                                           if (!cf.isSuccess()) {
                                               throw new IOException(
                                                       "Failed to respond successfully to message " + callable, cf.getCause());
                                           }
                                       }
                                   }
        );

        super.messageReceived(ctx, e);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        SpliceLogUtils.error(LOG, "exceptionCaught", e.getCause());
        super.exceptionCaught(ctx, e);
    }

}
