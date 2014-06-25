package com.splicemachine.si.impl.timestamp;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
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
import com.splicemachine.utils.SpliceLogUtils;

/**
 * TimestampClient implementation class that accepts concurrent client requests
 * (typically from {@link SITransactionManager} and sends them over a shared
 * connection to the remote {@link TimestampServer}.
 * <p>
 * This class should generally not be constructed directly.
 * Rather, {@link TimestampClientFactory} should be used 
 * to fetch the appropriate instance.
 * 
 * @author Walt Koetke
 */
public class TimestampClientMapImpl extends TimestampClient {

	private static final Logger LOG = Logger.getLogger(TimestampClientMapImpl.class);

	private static final short CLIENT_COUNTER_INIT = 100; // actual value doesn't matter

	/**
	 * Fixed number of bytes in the message we expect to receive back from the server.
	 */
	private static final int FIXED_MSG_RECEIVED_LENGTH = 10; // 2 byte client id + 8 byte timestamp
	
	private enum State {
		DISCONNECTED, CONNECTING, CONNECTED
    };

    /**
     * A map representing all currently active callers to this TimestampClient
     * waiting for their response.
     */
	private ConcurrentMap<Short, ClientCallback> _clientCallbacks = null;

    private AtomicReference<State> _state = new AtomicReference<State>(State.DISCONNECTED);

    private ClientBootstrap _bootstrap = null;
    
    private Channel _channel = null;
    
    private ChannelFactory _factory = null;
    
	/**
	 * Internal unique identifier for a single synchronous call to this instance
	 * of {@link TimestampClient}. Necessary in order to subsequently associate
	 * a server response with the original request.
	 */
    // We might even get away with using a byte here (256 concurrent client calls),
    // but use a short just in case.
    private volatile short _clientCallCounter = CLIENT_COUNTER_INIT;
   
    private int _port;

    public TimestampClientMapImpl() {

    	_port = SpliceConstants.timestampServerBindPort;
    	
    	_clientCallbacks = new ConcurrentHashMap<Short, ClientCallback>();
    	
    	_factory = new NioClientSocketChannelFactory(
			Executors.newCachedThreadPool(),
			Executors.newCachedThreadPool());
     
		_bootstrap = new ClientBootstrap(_factory);
		
    	// If we end up needing to use one of the memory aware executors,
    	// do so with code like this (leave commented out for reference).
    	//
	    // _bootstrap.getPipeline().addLast("executor", new ExecutionHandler(
		// 	   new OrderedMemoryAwareThreadPoolExecutor(10 /* threads */, 1024*1024, 4*1024*1024)));

		_bootstrap.getPipeline().addLast("decoder", new FixedLengthFrameDecoder(FIXED_MSG_RECEIVED_LENGTH));
		_bootstrap.getPipeline().addLast("handler", this);

		_bootstrap.setOption("tcpNoDelay", false);
		_bootstrap.setOption("keepAlive", true);
		_bootstrap.setOption("reuseAddress", true);
		// _bootstrap.setOption("connectTimeoutMillis", 120000);
		
		// Would be nice to try connecting here, but not sure if this works right.
		// connectIfNeeded();
    }

    protected String getHost() {
    	String hostName = null;
    	try {
			/*
			 * We have to use the deprecated API here because there appears to be no other way to get the host name
			 * and IP of the HMaster (short of going to ZooKeeper directly, at any rate).
			 */
			@SuppressWarnings("deprecation") HMasterInterface master = HConnectionManager.getConnection(SpliceConstants.config).getMaster();
			hostName = master.getClusterStatus().getMaster().getHostname();
    	} catch (Exception e) {
    		TimestampUtil.doClientErrorThrow(LOG, "Unable to determine host name for master.", e, null);
    	}
    	return hostName;
    }
    
    /**
     * Returns the port number which the client should use when connecting
     * to the timestamp server.
     */
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
	
			TimestampUtil.doClientInfo(LOG, "Attempting to connect to server (host " + getHost() + ", port " + getPort() + ")");
	
			ChannelFuture futureConnect = _bootstrap.connect(new InetSocketAddress(getHost(), getPort()));
			final CountDownLatch latchConnect = new CountDownLatch(1);
			futureConnect.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture cf) throws Exception {
 				    if (cf.isSuccess()) {
 				    	_channel = cf.getChannel();
						latchConnect.countDown();
				    } else {
				    	TimestampUtil.doClientErrorThrow(LOG, "TimestampClient unable to connect to TimestampServer", cf.getCause(), null);
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
    	
    	short clientCallId = incrementClientCounter();
    	final ClientCallback callback = new ClientCallback(clientCallId);
    	TimestampUtil.doClientDebug(LOG, "Starting new client call with id " + clientCallId);
    	
		// Add this caller (id and callback) to the map of current clients.
		// If an entry was already present for this caller id, that is a bug,
		// so throw an exception.
		if (_clientCallbacks.putIfAbsent(clientCallId, callback) != null) {
    		TimestampUtil.doClientErrorThrow(LOG, "Found existing client callback with caller id " + clientCallId +
				", so unable to process the new call", null, callback);
		}

		try {
            ChannelBuffer buffer = ChannelBuffers.buffer(4);
    	    buffer.writeShort(clientCallId);
			TimestampUtil.doClientTrace(LOG, "Writing request message to server", callback);
            ChannelFuture futureWrite = _channel.write(buffer);
	        futureWrite.addListener(new ChannelFutureListener() {
	            public void operationComplete(ChannelFuture future) {
	                if (!future.isSuccess()) {
	                    TimestampUtil.doClientErrorThrow(LOG, "Error writing message from timestamp client to server", future.getCause(), null);
	                } else {
	                    TimestampUtil.doClientTrace(LOG, "Request to server complete. Waiting for response.", callback);
	                }
	            }
	        });
    	} catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
        	_clientCallbacks.remove(clientCallId);
            callback.error(e);
    		TimestampUtil.doClientErrorThrow(LOG, "Exception writing message to timestamp server", e, callback);
    	}
        
    	// If we get here, request was successfully sent without exception.
    	// However, we might not have received response yet, so we need to
    	// wait for that now.
    	
		try {
			callback.await();
		} catch (InterruptedException e) {
            TimestampUtil.doClientErrorThrow(LOG, "Interrupted waiting for timestamp client callback", e, callback);
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

    private synchronized short incrementClientCounter() {
    	return ++_clientCallCounter;
    }
    
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
 		ChannelBuffer buf = (ChannelBuffer)e.getMessage();
 		assert (buf != null);
 		ensureReadableBytes(buf, FIXED_MSG_RECEIVED_LENGTH);
 		
 		short clientCallerId = buf.readShort();
 		assert (clientCallerId > 0);
 		ensureReadableBytes(buf, 8);
 		
 		long timestamp = buf.readLong();
 		assert (timestamp > 0);
 		ensureReadableBytes(buf, 0);

 		TimestampUtil.doClientDebug(LOG, "Response from server: clientCallerId = " + clientCallerId + ", timestamp = " + timestamp);
 		ClientCallback cb = _clientCallbacks.remove(clientCallerId);
        if (cb == null) {
        	TimestampUtil.doClientErrorThrow(LOG, "Client callback with id " + clientCallerId +
        	    " not found, so unable to deliver timestamp " + timestamp, null, null);
        }

        // This releases the latch the original client thread is waiting for
        // (to provide the synchronous behavior for that caller) and also
        // provides the timestamp.
        cb.complete(timestamp);
        
        super.messageReceived(ctx,  e);
	}

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    	TimestampUtil.doClientInfo(LOG, "Successfully connected to server");
        synchronized(_state) {
           _channel = e.getChannel();
           _state.set(State.CONNECTED);
        }
        super.channelConnected(ctx, e);
    }

    protected void doTrace(String message) {
    	TimestampUtil.doClientTrace(LOG, message);
	}

    protected void doDebug(String message) {
    	TimestampUtil.doClientDebug(LOG, message);
	}

	protected void doError(String message, Throwable t) {
    	TimestampUtil.doClientError(LOG, message, t);
    }
}
