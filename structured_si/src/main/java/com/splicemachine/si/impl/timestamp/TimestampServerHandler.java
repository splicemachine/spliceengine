package com.splicemachine.si.impl.timestamp;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;

import com.splicemachine.constants.SpliceConstants;

public class TimestampServerHandler extends TimestampBaseHandler {
	
    private static final Logger LOG = Logger.getLogger(TimestampServerHandler.class);

    private RecoverableZooKeeper _rzk;
	
	private TimestampOracle _oracle = null;
	
	public TimestampServerHandler(RecoverableZooKeeper rzk) {
		super();
		_rzk = rzk;
	}

	// TODO: better to do this from constructor but there are timing issues with this and zookeeper
	public void initializeIfNeeded() {
		TimestampUtil.doServerDebug(LOG, "initializeIfNeeded");
		synchronized(this) {
			if (_oracle == null) {
				_oracle = TimestampOracle.getInstance(_rzk, SpliceConstants.zkSpliceMaxReservedTimestampPath);
			}
		}
	}
	
    @Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    	assert _oracle != null;
    	assert _rzk != null;
    	
		ChannelBuffer buf = (ChannelBuffer)e.getMessage();
		assert buf != null;
 		assert (buf.readableBytes() == 4);
		
		final int callerId = buf.readInt();
		assert callerId > 0;
		
		doDebug("messageReceived: fetching next timestamp for caller " + callerId);
		long nextTimestamp = _oracle.getNextTimestamp();
 		assert (buf.readableBytes() == 0);
		
		//
		// Respond to the client
		//

		ChannelBuffer writeBuf = ChannelBuffers.buffer(12); // 4 for caller id, 8 for timestamp
		writeBuf.writeInt(callerId);
		writeBuf.writeLong(nextTimestamp);
		Channel channel = e.getChannel();
		doDebug("messageReceived: writing timestamp " + nextTimestamp + " to client caller " + callerId + ", writable = " + channel.isWritable());
		// Two ways two write: Channels.write and e.getChannel().write.
		// Keep both around for now and pick a winner later
		// ChannelFuture futureResponse = Channels.write(channel, writeBuf, channel.getRemoteAddress());
        ChannelFuture futureResponse = e.getChannel().write(writeBuf);
		futureResponse.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture cf) throws Exception {
			    if (cf.isSuccess()) {
		    		doDebug("messageReceived: writing to client (caller id " + callerId + ") complete.");
			    } else {
			    	throw new RuntimeException(
		    			"Something went wrong writing response back to TimestampClient", cf.getCause());
			    }																					
			  }
			}
		);
		
		super.messageReceived(ctx, e);
	}
		   
    protected void doDebug(String message) {
    	TimestampUtil.doServerDebug(LOG, message);
	}

	protected void doError(String message) {
    	TimestampUtil.doServerError(LOG, message);
    }

}
