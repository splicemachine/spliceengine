package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampIOException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

public abstract class TimestampBaseHandler extends SimpleChannelHandler {

    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    	doError("exceptionCaught", e.getCause());
    	super.exceptionCaught(ctx, e);
    }

    protected void ensureReadableBytes(ChannelBuffer buf, int expected) throws TimestampIOException{
 		if (buf.readableBytes() != expected) {
 			throw new TimestampIOException("Invalid number of readable bytes " + buf.readableBytes() +
				" where " + expected + " was expected.");
 		}
    }
    
    protected abstract void doError(String message, Throwable t, Object... args);
}
