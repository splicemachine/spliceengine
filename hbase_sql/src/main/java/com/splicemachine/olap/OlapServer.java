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

package com.splicemachine.olap;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.utils.SpliceLogUtils;
import io.netty.bootstrap.ChannelFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OlapServer {
    private static final Logger LOG = Logger.getLogger(OlapServer.class);

    private int port;
    private Clock clock;
    private Channel channel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public OlapServer(int port,Clock clock) {
        this.port = port;
        this.clock=clock;
    }

    public void startServer(SConfiguration config) throws IOException {

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(15, new ThreadFactoryBuilder().setNameFormat("OlapServer-%d").setDaemon(true).build());

        SpliceLogUtils.warn(LOG, "Olap Server starting (binding to port %s)...", port);

        ServerBootstrap bootstrap = new ServerBootstrap();

        // Instantiate handler once and share it
        OlapJobRegistry registry = new MappedJobRegistry(config.getOlapClientTickTime(),
                config.getOlapServerTickLimit(),
                TimeUnit.MILLISECONDS);
        ChannelInboundHandler submitHandler = new OlapRequestHandler(config,
                registry,clock,config.getOlapClientTickTime());
        ChannelInboundHandler statusHandler = new OlapStatusHandler(registry);
        ChannelInboundHandler cancelHandler = new OlapCancelHandler(registry);

        bossGroup = new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("OlapServer-boss-%d").setDaemon(true).build());
        workerGroup = new NioEventLoopGroup(15, new ThreadFactoryBuilder().setNameFormat("OlapServer-%d").setDaemon(true).build());
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new OlapPipelineFactory(submitHandler,cancelHandler,statusHandler));
        bootstrap.option(ChannelOption.TCP_NODELAY, false);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, false);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.childOption(ChannelOption.SO_REUSEADDR, true);

        int portToTry = getPortNumber();
        while(true) {
            try {
                this.channel = bootstrap.bind(new InetSocketAddress(portToTry)).sync().channel();
                // channel is bound, break from retry loop
                break;
            } catch (InterruptedException e) {
                throw new IOException(e);
            } catch (Exception e){
                if (e instanceof BindException && e.getMessage().contains("Address already in use")) {
                    // try next port number
                } else {
                    throw e;
                }
            }
            // Increase port number and try again
            portToTry++;
        }
        port = ((InetSocketAddress)channel.localAddress()).getPort();

        SpliceLogUtils.warn(LOG, "Olap Server started at port " + port);

    }

    private int getPortNumber() {
        return port;
    }

    int getBoundPort() {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    String getBoundHost() {
        return ((InetSocketAddress) channel.localAddress()).getHostName();
    }

    void stopServer() {
        try {
            this.channel.close();
        } catch (Exception e) {
            LOG.error("unexpected exception during stop server", e);
        }
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }
}
