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
    	doDebug("exceptionCaught");
    	e.getCause().printStackTrace();
    	super.exceptionCaught(ctx, e);
    }

    public void channelOpen(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelOpen");
    	super.channelOpen(ctx, e);
    }

    public void channelBound(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelBound");
    	super.channelBound(ctx, e);
    }

    public void channelConnected(
    	ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelConnected");
        super.channelConnected(ctx, e);
    }

    public void channelInterestChanged(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelInterestChanged");
    	super.channelInterestChanged(ctx, e);
    }

    public void channelDisconnected(
		ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelDisconnected");
    	super.channelDisconnected(ctx, e);
    }

    public void channelUnbound(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelUnbound");
    	super.channelUnbound(ctx, e);
    }

    public void channelClosed(
        ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	doDebug("channelClosed");
    	super.channelClosed(ctx, e);
    }

    public void writeComplete(
    	ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
    	doDebug("writeComplete");
    	super.writeComplete(ctx, e);
   }

    public void childChannelOpen(
        ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
    	doDebug("childChannelOpen");
    	super.childChannelOpen(ctx, e);
    }

    public void childChannelClosed(
        ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
    	doDebug("childChannelClosed");
    	super.childChannelClosed(ctx, e);
    }

    void doDebug(String message) {
	}
	
    void doError(String message) {
	}
}
