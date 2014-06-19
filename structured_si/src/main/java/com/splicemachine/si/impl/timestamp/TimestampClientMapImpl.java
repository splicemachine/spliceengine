package com.splicemachine.si.impl.timestamp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;

import com.splicemachine.constants.SpliceConstants;

public class TimestampClientMapImpl extends TimestampClient {

	// TODO: review each bootstrap.setOption value

	// TODO: throw more specific exception type like TimestampException (instead of just IOException/RuntimeException)?
	
	private static final Logger LOG = Logger.getLogger(TimestampClientMapImpl.class);

	private final int CLIENT_COUNTER_INIT = 100; // actual value doesn't matter

    private enum State {
		DISCONNECTED, CONNECTING, CONNECTED
    };

	private ConcurrentHashMap<Integer, ClientCallback> _clientCallbacks = null;

    private AtomicReference<State> _state = new AtomicReference<State>(State.DISCONNECTED);

    private ClientBootstrap _bootstrap = null;
    
    private Channel _channel = null;
    
    private ChannelFactory _factory = null;
    
    private AtomicInteger _clientCallCounter = null;
    
    private int _port;

    public TimestampClientMapImpl(int port) {

    	_port = port;
    	
    	_clientCallbacks = new ConcurrentHashMap<Integer, ClientCallback>();
    	
    	_clientCallCounter = new AtomicInteger(CLIENT_COUNTER_INIT);
    	
    	_factory = new NioClientSocketChannelFactory(
			Executors.newCachedThreadPool(),
			Executors.newCachedThreadPool());
     
		_bootstrap = new ClientBootstrap(_factory);
		
	    // _bootstrap.getPipeline().addLast("executor", new ExecutionHandler(
		// 	   new OrderedMemoryAwareThreadPoolExecutor(10 /* threads */, 1024*1024, 4*1024*1024)));

		_bootstrap.getPipeline().addLast("decoder", new FixedLengthFrameDecoder(12)); // We receive 4 byte id plus 8 byte ts from server
		_bootstrap.getPipeline().addLast("handler", this);

		_bootstrap.setOption("tcpNoDelay", false);
		_bootstrap.setOption("keepAlive", true);
		_bootstrap.setOption("reuseAddress", true);
		// _bootstrap.setOption("connectTimeoutMillis", 120000);
    }

    protected String getHost() {
    	String hostName;
    	try {
    		hostName = HConnectionManager.getConnection(SpliceConstants.config).getMaster().getClusterStatus().getMaster().getHostname();
    	} catch (Exception e) {
    		TimestampUtil.doClientError(LOG, "Unable to determine host name for master. Using localhost as workaround but this is not correct.", null, e);
    		throw new RuntimeException("Unable to determine host name", e);
    	}
    	return hostName;
    }
    
    protected int getPort() {
    	return _port;
    }

    protected State connectIfNeeded() {

    	// Even though state is an atomic reference, synchronize on whole block
    	// including code that attempts connection. Otherwise, two threads might
    	// end up trying to connect at the same time.
    	
    	synchronized(_state) {
		
            if (_state.get() == State.CONNECTED || _state.get() == State.CONNECTING) {
            	return _state.get();
            }
	
			TimestampUtil.doClientInfo(LOG, "Attempting to connect to server (port " + getPort() + ")...");
	
			ChannelFuture futureConnect = _bootstrap.connect(new InetSocketAddress(getHost(), getPort()));
			final CountDownLatch latchConnect = new CountDownLatch(1);
			futureConnect.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture cf) throws Exception {
 				    if (cf.isSuccess()) {
 				    	_channel = cf.getChannel();
						latchConnect.countDown();
				    } else {
				    	TimestampUtil.doClientError(LOG, "TimestampClient unable to connect to TimestampServer", cf.getCause());
				    	throw new RuntimeException("TimestampClient unable to connect to TimestampServer", cf.getCause());
				    }
				  }
				}
			);
	
    	
			try {
				latchConnect.await();
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted waiting for timestamp client to connect to server", e);
			}

			// Can only assume connecting (not connected) until channelConnected method is invoked
			_state.set(State.CONNECTING);

			return _state.get();
    	}
    }
    
    public long getNextTimestamp() {

    	connectIfNeeded();
    	
    	int clientCallId = _clientCallCounter.incrementAndGet();
    	final ClientCallback callback = new ClientCallback(clientCallId);
    	TimestampUtil.doClientDebug(LOG, "Starting new client call", callback);
    	
    	try {
    	    // Don't need to synchronize because _clientCallbacks is implicitly synchronized.
    		// Should we check whether it already exists in the map?
    		_clientCallbacks.put(clientCallId, callback);
	
            ChannelBuffer buffer = ChannelBuffers.buffer(4);
    	    buffer.writeInt(clientCallId);
			TimestampUtil.doClientDebug(LOG, "Writing request message to server", callback);
            ChannelFuture futureWrite = _channel.write(buffer);
	        futureWrite.addListener(new ChannelFutureListener() {
	            public void operationComplete(ChannelFuture future) {
	                if (!future.isSuccess()) {
	                   callback.error(new IOException("Error writing to socket from timestamp client"));
	                   throw new RuntimeException("Error writing message from timestamp client to server", future.getCause());
	                } else {
	                   TimestampUtil.doClientDebug(LOG, "write to server operation complete", callback);
	                }
	            }
	        });
    	} catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
    		TimestampUtil.doClientError(LOG, "Got exception writing to server. Discarding callback", callback, e);
        	_clientCallbacks.remove(clientCallId);
            callback.error(e);
        	throw new RuntimeException("Error occurred trying to write message to timestamp server", e);
    	}
        
		try {
			TimestampUtil.doClientDebug(LOG, "Waiting for response from server", callback);
			callback.await();
		} catch (InterruptedException e) {
            TimestampUtil.doClientError(LOG, "Interrupted or expired waiting for timestamp client callback", callback, e);
        	throw new RuntimeException("Interrupted or expired waiting for timestamp client callback", e);
		}
    	
		// If we get here, it should mean the client received the response with the timestamp,
		// which we can fetch now from the callback and send it back to the caller.
		
		long timestamp = callback.getNewTimestamp();
        if (timestamp < 0) {
        	throw new RuntimeException("Invalid timestamp created in timestamp callback " + callback);
        }
    	
        TimestampUtil.doClientDebug(LOG, "Client call complete", callback);
    	
    	return timestamp;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    	ChannelBuffer buf = (ChannelBuffer)e.getMessage();
 		assert (buf != null);
 		assert (buf.readableBytes() == 12);
 		
 		int clientCallerId = buf.readInt();
 		assert (clientCallerId > 0);
 		assert (buf.readableBytes() == 8);
 		
 		long timestamp = buf.readLong();
 		assert (timestamp > 0);
 		assert (buf.readableBytes() == 0);

 		TimestampUtil.doClientDebug(LOG, "messageReceived: clientCallerId = " + clientCallerId + " : timestamp = " + timestamp);
 		ClientCallback cb = _clientCallbacks.remove(clientCallerId);
        if (cb == null) {
        	TimestampUtil.doClientError(LOG, "No client callback was found even though client received a timestamp: " + timestamp);
        	throw new RuntimeException("No client callback was found even though client received a timestamp: " + timestamp);
        }

        cb.complete(timestamp);
        
        super.messageReceived(ctx,  e);
	}

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	TimestampUtil.doClientInfo(LOG, "client successfully connected to server");
        synchronized(_state) {
           _channel = e.getChannel();
           _state.set(State.CONNECTED);
        }
        super.channelConnected(ctx, e);
    }

    protected void doDebug(String message) {
    	TimestampUtil.doClientDebug(LOG, message);
	}

	protected void doError(String message) {
    	TimestampUtil.doClientError(LOG, message);
    }

	
}
