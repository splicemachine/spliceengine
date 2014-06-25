package com.splicemachine.si.impl.timestamp;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

/**
 * Accepts concurrent requests for new transactional timestamps and
 * sends them over a shared connection to the remote {@link TimestampServer}.
 * For the caller, the invocation of {@link #getNextTimestamp()}
 * is synchronous.
 * <p>
 * This class should generally not be constructed directly.
 * Rather, {@link TimestampClientFactory} should be used 
 * to fetch the appropriate instance.
 * 
 * @author Walt Koetke
 */
public class TimestampClient extends TimestampBaseHandler implements TimestampRegionManagement {


	private static final Logger LOG = Logger.getLogger(TimestampClient.class);

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
	 * a server response with the original request. Although this is an atomic integer,
	 * we consume it internally as a short so that we only pass two bytes (not four)
	 * over the wire.
	 */
    // We might even get away with using a byte here (256 concurrent client calls),
    // but use a short just in case.
    private AtomicInteger _clientCallCounter = new AtomicInteger(CLIENT_COUNTER_INIT);
   
    private int _port;

	// Metrics to expose via JMX. See TimestampRegionManagement
	// for solid definitions of each metric.
    private AtomicLong _numRequests = new AtomicLong(0);
    private AtomicLong _totalRequestDuration = new AtomicLong(0);

    public TimestampClient() {

    	_port = SpliceConstants.timestampServerBindPort;
    	
    	_clientCallbacks = new ConcurrentHashMap<Short, ClientCallback>();

        ExecutorService workerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-worker-%d").setDaemon(true).build());
        ExecutorService bossExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-boss-%d").setDaemon(true).build());
    	_factory = new NioClientSocketChannelFactory(
              bossExecutor,workerExecutor);

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
		
		try {
			registerJMX();
		} catch (Exception e) {
	        TimestampUtil.doServerError(LOG, "Unable to register TimestampClient with JMX. Timestamps will still be generated but metrics will not be available.");
		}
    }

    protected String getHost() throws TimestampIOException {
    	String hostName = null;
    	try {
			/*
			 * We have to use the deprecated API here because there appears to be no other way to get the host name
			 * and IP of the HMaster (short of going to ZooKeeper directly, at any rate).
			 */
			@SuppressWarnings("deprecation") HMasterInterface master = HConnectionManager.getConnection(SpliceConstants.config).getMaster();
			hostName = master.getClusterStatus().getMaster().getHostname();
			
			// Would this be better?
			// SpliceUtilities.getAdmin().getClusterStatus().getMaster().getHostname();
    	} catch (Exception e) {
    		TimestampUtil.doClientErrorThrow(LOG, "Unable to determine host name for active hbase master", e);
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

    protected State connectIfNeeded() throws TimestampIOException {

    	// Even though state is an atomic reference, synchronize on whole block
    	// including code that attempts connection. Otherwise, two threads might
    	// end up trying to connect at the same time.
    	
    	synchronized(_state) {
		
            if (_state.get() == State.CONNECTED || _state.get() == State.CONNECTING) {
            	return _state.get();
            }
	
            if (LOG.isInfoEnabled()) {
			    TimestampUtil.doClientInfo(LOG, "Attempting to connect to server (host %s, port %s)", getHost(), getPort());
            }
            
			ChannelFuture futureConnect = _bootstrap.connect(new InetSocketAddress(getHost(), getPort()));
			final CountDownLatch latchConnect = new CountDownLatch(1);
			futureConnect.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture cf) throws Exception {
 				    if (cf.isSuccess()) {
 				    	_channel = cf.getChannel();
						latchConnect.countDown();
				    } else {
				    	TimestampUtil.doClientErrorThrow(LOG, "TimestampClient unable to connect to TimestampServer", cf.getCause());
				    }
				  }
				}
			);
	
 			try {
				latchConnect.await();
			} catch (InterruptedException e) {
				throw new TimestampIOException("Interrupted waiting for timestamp client to connect to server", e);
			}

			// Can only assume connecting (not connected) until channelConnected method is invoked
			_state.set(State.CONNECTING);

			return _state.get();
    	}
    }
    
    public long getNextTimestamp() throws TimestampIOException {

    	// Measure duration of full client request for JMX
        long requestStartTime = System.currentTimeMillis();

    	connectIfNeeded();
    	
    	short clientCallId = (short)_clientCallCounter.getAndIncrement();
    	final ClientCallback callback = new ClientCallback(clientCallId);
    	TimestampUtil.doClientDebug(LOG, "Starting new client call with id %s", clientCallId);
    	
		// Add this caller (id and callback) to the map of current clients.
		// If an entry was already present for this caller id, that is a bug,
		// so throw an exception.
		if (_clientCallbacks.putIfAbsent(clientCallId, callback) != null) {
    		TimestampUtil.doClientErrorThrow(LOG, "Found existing client callback with caller id %s, so unable to handle new call.", null, clientCallId);
		}

		try {
            ChannelBuffer buffer = ChannelBuffers.buffer(2);
    	    buffer.writeShort(clientCallId);
			TimestampUtil.doClientTrace(LOG, "Writing request message to server for client: %s", callback);
            ChannelFuture futureWrite = _channel.write(buffer);
	        futureWrite.addListener(new ChannelFutureListener() {
	            public void operationComplete(ChannelFuture future) throws Exception {
	                if (!future.isSuccess()) {
	                    TimestampUtil.doClientErrorThrow(LOG, "Error writing message from timestamp client to server", future.getCause());
	                } else {
	                    TimestampUtil.doClientTrace(LOG, "Request sent. Waiting for response for client: %s", callback);
	                }
	            }
	        });
    	} catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
        	_clientCallbacks.remove(clientCallId);
            callback.error(e);
    		TimestampUtil.doClientErrorThrow(LOG, "Exception writing message to timestamp server for client: %s", e, callback);
    	}
        
    	// If we get here, request was successfully sent without exception.
    	// However, we might not have received response yet, so we need to
    	// wait for that now.
    	
		try {
			int timeoutMillis = SpliceConstants.timestampClientWaitTime;
			boolean success = callback.await(timeoutMillis);
			if (!success) {
			    TimestampUtil.doClientErrorThrow(LOG, "Client timed out after %s ms waiting for new timestamp: %s", null, timeoutMillis, callback);
			}
		} catch (InterruptedException e) {
            TimestampUtil.doClientErrorThrow(LOG, "Interrupted waiting for timestamp client: %s", e, callback);
		}
    	
		// If we get here, it should mean the client received the response with the timestamp,
		// which we can fetch now from the callback and send it back to the caller.
		
		long timestamp = callback.getNewTimestamp();
        if (timestamp < 0) {
        	TimestampUtil.doClientErrorThrow(LOG, "Invalid timestamp found for client: %s", null, callback);
        }
    	
        TimestampUtil.doClientDebug(LOG, "Client call complete: %s", callback);
    	
        // Since request was successful, update JMX metrics
        _numRequests.incrementAndGet();
        _totalRequestDuration.addAndGet(System.currentTimeMillis() - requestStartTime);
        
    	return timestamp;
    }

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
 		ChannelBuffer buf = (ChannelBuffer)e.getMessage();
 		assert (buf != null);
 		ensureReadableBytes(buf, FIXED_MSG_RECEIVED_LENGTH);
 		
 		short clientCallerId = buf.readShort();
 		ensureReadableBytes(buf, 8);
 		
 		long timestamp = buf.readLong();
 		assert (timestamp > 0);
 		ensureReadableBytes(buf, 0);

 		TimestampUtil.doClientDebug(LOG, "Response from server: clientCallerId = %s, timestamp = %s", clientCallerId, timestamp);
 		ClientCallback cb = _clientCallbacks.remove(clientCallerId);
        if (cb == null) {
        	TimestampUtil.doClientErrorThrow(LOG, "Client callback with id %s not found, so unable to deliver timestamp %s", null, clientCallerId, timestamp);
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

    protected void doTrace(String message, Object... args) {
    	TimestampUtil.doClientTrace(LOG, message, args);
	}

    protected void doDebug(String message, Object... args) {
    	TimestampUtil.doClientDebug(LOG, message, args);
	}

	protected void doError(String message, Throwable t, Object... args) {
    	TimestampUtil.doClientError(LOG, message, t, args);
    }

	private void registerJMX() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        registerJMX(mbs);
        TimestampUtil.doServerInfo(LOG, "TimestampClient on region server successfully registered with JMX");
	}
	
    private void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName name = new ObjectName("com.splicemachine.si.impl.timestamp.request:type=TimestampRegionManagement"); // Same string is in JMXUtils
        mbs.registerMBean(this, name);
    }

	public long getNumberTimestampRequests() {
		return _numRequests.get();
	}
	
 	public double getAvgTimestampRequestDuration() {
 		double a = (double)_totalRequestDuration.get();
 		double b = (double)_numRequests.get();
 		return a / b;
 	}

}
