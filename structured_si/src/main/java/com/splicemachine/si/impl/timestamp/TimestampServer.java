package com.splicemachine.si.impl.timestamp;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

public class TimestampServer implements Runnable {
    private static final Logger LOG = Logger.getLogger(TimestampServer.class);

	private RecoverableZooKeeper _rzk;
	private int _port;
	
	public TimestampServer(int port, RecoverableZooKeeper rzk) {
		_port = port;
		_rzk = rzk;
	}
	
	public void run() {
		startServer();
	}
	
    public void startServer() {

    	ChannelFactory factory = new NioServerSocketChannelFactory(
			Executors.newCachedThreadPool(),
			Executors.newCachedThreadPool());

    	TimestampUtil.doServerInfo(LOG, "Timestamp Server starting (binding to port " + _port + ")...");

    	ServerBootstrap bootstrap = new ServerBootstrap(factory);

    	// Instantiate handler once and share it
    	final TimestampServerHandler handler = new TimestampServerHandler(_rzk);
    	
        //final ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(
    	//	5 /* threads */, 1048576, 1073741824,
        //    100, TimeUnit.MILLISECONDS, // Default would have been 30 SECONDS
        //    Executors.defaultThreadFactory());

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
	
    protected void doDebug(String s) {
    	TimestampUtil.doServerDebug(LOG, s);
    }
    
    // Only used by main method when testing
    /*
    private static final int numZkRetries = 3;
    private static final int zkRetryPauseMs = 1000;
    private static final int zkSessionTimeoutMs = 10000;

    private static RecoverableZooKeeper getZooKeeper()  {
        try {
        	RecoverableZooKeeper rzk = new RecoverableZooKeeper(
            	"localhost:2181",
            	TimestampServer.zkSessionTimeoutMs,
            	new Watcher() {
            		@Override
            		public void process(WatchedEvent watchedEvent) {
            			// System.out.println(watchedEvent);
            		}
            	},
            	TimestampServer.numZkRetries,
            	TimestampServer.zkRetryPauseMs);
        	return rzk;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
    	new Thread(new TimestampServer(getZooKeeper())).start();
    	Thread.sleep(10000);
    }
    */
}
