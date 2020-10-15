/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.timestamp.impl;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

public class TimestampServer {
    private static final Logger LOG = Logger.getLogger(TimestampServer.class);

    private int port;
    private Channel channel;
    private final TimestampServerHandler handler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public TimestampServer(int port, TimestampServerHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public void startServer() {
        bossGroup = new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("TimestampServer-boss-%d").setDaemon(true).build());
        workerGroup = new NioEventLoopGroup(5, new ThreadFactoryBuilder().setNameFormat("TimestampServer-worker-%d").setDaemon(true).build());

        SpliceLogUtils.info(LOG, "Timestamp Server starting (binding to port %s)...", port);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(port)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childHandler(new TimestampPipelineFactoryLite(handler));

        try {
            ChannelFuture f = bootstrap.bind().sync();
            channel = f.channel();
        } catch (InterruptedException e) {
            LOG.error("Interrupted while starting", e);
            throw new RuntimeException(e);
        }

        SpliceLogUtils.info(LOG, "Timestamp Server started.");
    }

    protected int getPortNumber() {
        return port;
    }

    int getBoundPort() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    public void stopServer() throws InterruptedException {
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();

        // Wait until all threads are terminated.
        bossGroup.terminationFuture().sync();
        workerGroup.terminationFuture().sync();
    }
}
