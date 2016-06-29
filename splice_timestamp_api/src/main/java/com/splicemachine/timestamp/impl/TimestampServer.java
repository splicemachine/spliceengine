package com.splicemachine.timestamp.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.bootstrap.ServerBootstrap;
import org.sparkproject.jboss.netty.channel.Channel;
import org.sparkproject.jboss.netty.channel.ChannelFactory;
import org.sparkproject.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

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

    private int port;
    private ChannelFactory factory;
    private Channel channel;
    private TimestampBlockManager timestampBlockManager;
    private int blockSize;

    public TimestampServer(int port, TimestampBlockManager timestampBlockManager, int blockSize) {
        this.port = port;
        this.timestampBlockManager=timestampBlockManager;
        this.blockSize = blockSize;
    }

    public void startServer() {

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("TimestampServer-%d").setDaemon(true).build());
        this.factory = new NioServerSocketChannelFactory(executor, executor);

        SpliceLogUtils.info(LOG, "Timestamp Server starting (binding to port %s)...", port);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        // Instantiate handler once and share it
        final TimestampServerHandler handler = new TimestampServerHandler(timestampBlockManager,blockSize);

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

        this.channel = bootstrap.bind(new InetSocketAddress(getPortNumber()));

        SpliceLogUtils.info(LOG, "Timestamp Server started.");
    }

    protected int getPortNumber() {
        return port;
    }

    public void stopServer() {
        try {
            this.channel.close().await(5000);
        } catch (Exception e) {
            LOG.error("unexpected exception during stop server", e);
        }
        this.factory.shutdown();
    }
}
