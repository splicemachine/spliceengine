/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class OlapServer {
    private static final Logger LOG = Logger.getLogger(OlapServer.class);

    private int port;
    private Clock clock;
    private ChannelFactory factory;
    private Channel channel;

    public OlapServer(int port,Clock clock) {
        this.port = port;
        this.clock=clock;
    }

    public void startServer(SConfiguration config) {

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(15, new ThreadFactoryBuilder().setNameFormat("OlapServer-%d").setDaemon(true).build());
        this.factory = new NioServerSocketChannelFactory(executor, 2, executor, 10);

        SpliceLogUtils.info(LOG, "Olap Server starting (binding to port %s)...", port);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        // Instantiate handler once and share it
        OlapJobRegistry registry = new MappedJobRegistry(config.getOlapClientTickTime(),
                config.getOlapServerTickLimit(),
                TimeUnit.MILLISECONDS);
        ChannelHandler submitHandler = new OlapRequestHandler(config,
                registry,clock,config.getOlapClientTickTime());
        ChannelHandler statusHandler = new OlapStatusHandler(registry);
        ChannelHandler cancelHandler = new OlapCancelHandler(registry);

        bootstrap.setPipelineFactory(new OlapPipelineFactory(submitHandler,cancelHandler,statusHandler));
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);

        this.channel = bootstrap.bind(new InetSocketAddress(getPortNumber()));
        ((InetSocketAddress)channel.getLocalAddress()).getPort();

        SpliceLogUtils.info(LOG, "Olap Server started.");

    }

    private int getPortNumber() {
        return port;
    }

    int getBoundPort() {
        return ((InetSocketAddress) channel.getLocalAddress()).getPort();
    }

    String getBoundHost() {
        return ((InetSocketAddress) channel.getLocalAddress()).getHostName();
    }

    void stopServer() {
        try {
            this.channel.close().await(5000);
        } catch (Exception e) {
            LOG.error("unexpected exception during stop server", e);
        }
        this.factory.shutdown();
    }
}
