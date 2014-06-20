package com.splicemachine.si.impl.timestamp;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

public abstract class TimestampBaseHandler extends SimpleChannelHandler {

    public void exceptionCaught(
		ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	doError("exceptionCaught", e.getCause());
    	super.exceptionCaught(ctx, e);
    }

    public void channelOpen(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelOpen");
    	super.channelOpen(ctx, e);
    }

    public void channelBound(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelBound");
    	super.channelBound(ctx, e);
    }

    public void channelConnected(
    	ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelConnected");
        super.channelConnected(ctx, e);
    }

    public void channelInterestChanged(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelInterestChanged");
    	super.channelInterestChanged(ctx, e);
    }

    public void channelDisconnected(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelDisconnected");
    	super.channelDisconnected(ctx, e);
    }

    public void channelUnbound(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelUnbound");
    	super.channelUnbound(ctx, e);
    }

    public void channelClosed(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doTrace("channelClosed");
    	super.channelClosed(ctx, e);
    }

    public void writeComplete(
    	ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
    	doTrace("writeComplete");
    	super.writeComplete(ctx, e);
   }

    public void childChannelOpen(
        ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
    	doTrace("childChannelOpen");
    	super.childChannelOpen(ctx, e);
    }

    public void childChannelClosed(
        ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
    	doTrace("childChannelClosed");
    	super.childChannelClosed(ctx, e);
    }

    protected abstract void doTrace(String message);
    
    protected abstract void doDebug(String message);
	
    protected abstract void doError(String message, Throwable t);
}
