package com.splicemachine.si.impl.timestamp;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class TimestampServer {
    private static final Logger LOG = Logger.getLogger(TimestampServer.class);

	/**
	 * Fixed number of bytes in the message we expect to receive from the client.
	 */
	static final int FIXED_MSG_RECEIVED_LENGTH = 2; // 2 byte client id
	
	/**
	 * Fixed number of bytes in the message we expect to send back to the client.
	 */
	static final int FIXED_MSG_SENT_LENGTH = 10; // 2 byte client id + 8 byte timestamp
	
	private RecoverableZooKeeper _rzk;
	private int _port;
	
	public TimestampServer(int port, RecoverableZooKeeper rzk) {
		_port = port;
		_rzk = rzk;
	}
	
    public void startServer() {

    	ChannelFactory factory = new NioServerSocketChannelFactory(
			Executors.newCachedThreadPool(),
			Executors.newCachedThreadPool());

    	TimestampUtil.doServerInfo(LOG, "Timestamp Server starting (binding to port " + _port + ")...");

    	ServerBootstrap bootstrap = new ServerBootstrap(factory);

    	// Instantiate handler once and share it
    	final TimestampServerHandler handler = new TimestampServerHandler(_rzk);
    	
    	// If we end up needing to use one of the memory aware executors,
    	// do so with code like this (leave commented out for reference).
    	// But for now we can use the 'lite' implementation.
    	//
        // final ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
    	//     5 /* threads */, 1048576, 1073741824,
        //     100, TimeUnit.MILLISECONDS, // Default would have been 30 SECONDS
        //     Executors.defaultThreadFactory());
        // bootstrap.setPipelineFactory(new TimestampPipelineFactory(pipelineExecutor, handler));

    	bootstrap.setPipelineFactory(new TimestampPipelineFactoryLite(handler));
        
        bootstrap.setOption("tcpNoDelay", false);
        // bootstrap.setOption("child.sendBufferSize", 1048576);
        // bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        // bootstrap.setOption("child.connectTimeoutMillis", 120000);

		bootstrap.bind(new InetSocketAddress(getPortNumber()));
		
		TimestampUtil.doServerInfo(LOG, "Timestamp Server started.");
	}
	
	protected int getPortNumber() {
		return _port;
	}
}
