package com.splicemachine.olap;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class OlapServer {
    private static final Logger LOG = Logger.getLogger(OlapServer.class);

    private int port;
    private ChannelFactory factory;
    private Channel channel;

    public OlapServer(int port) {
        this.port = port;
    }

    public void startServer() {

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("OlapServer-%d").setDaemon(true).build());
        this.factory = new NioServerSocketChannelFactory(executor, executor);

        SpliceLogUtils.info(LOG, "Olap Server starting (binding to port %s)...", port);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        // Instantiate handler once and share it
        final OlapServerHandler handler = new OlapServerHandler();

        bootstrap.setPipelineFactory(new OlapPipelineFactory(handler));

        bootstrap.setOption("tcpNoDelay", false);
        // bootstrap.setOption("child.sendBufferSize", 1048576);
        // bootstrap.setOption("child.receiveBufferSize", 1048576);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        // bootstrap.setOption("child.connectTimeoutMillis", 120000);

        this.channel = bootstrap.bind(new InetSocketAddress(getPortNumber()));
        ((InetSocketAddress)channel.getLocalAddress()).getPort();

        SpliceLogUtils.info(LOG, "Olap Server started.");
    }

    protected int getPortNumber() {
        return port;
    }

    public int getBoundPort() {
        return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    }

    public String getBoundHost() {
        return ((InetSocketAddress) channel.getLocalAddress()).getHostName();
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
