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

package com.splicemachine.stream;

import com.splicemachine.stream.handlers.OpenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * OlapStreamListener handles a communication from StreamListener to manage throttling if a client overloaded with
 * messages or paused consuming.
 */
public class OlapStreamListener extends ChannelInboundHandlerAdapter {
    private static final Logger LOG = Logger.getLogger(OlapStreamListener.class);

    private final CountDownLatch active;
    private final UUID uuid;
    private final String host;
    private final int port;
    volatile private boolean clientConsuming;
    private final Lock lock;
    private final Condition consumingCondition;

    public OlapStreamListener(String host, int port, UUID uuid) {
        this.active = new CountDownLatch(1);
        this.host = host;
        this.port = port;
        this.uuid = uuid;
        this.clientConsuming = true;
        this.lock = new ReentrantLock();
        this.consumingCondition = lock.newCondition();
    }

    /**
     * in {@link #clientConsuming} the client consuming state from StreamListener is stored. If the state is changed on StreamListener,
     * it sends StreamProtocol.PauseStream or StreamProtocol.ContinueStream message and clientConsuming is updated, see
     * {@link #channelRead}.
     *
     * @return consuming state
     */
    public boolean isClientConsuming() {
        return clientConsuming;
    }

    public Lock getLock() {
        return lock;
    }

    /**
     * {@link #consumingCondition} is used to inform a class, that uses OlapStreamListener e.g. StreamableRDD,
     * that the state has changed to repeat a loop checking for other conditions.
     *
     * @return lock condition
     */
    public Condition getCondition() {
        return consumingCondition;
    }

    public void createChannelToStreamListener() {
        InetSocketAddress socketAddr = new InetSocketAddress(host, port);
        Bootstrap bootstrap;
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("OlapStreamListener-" + host + ":" + port + "[" + uuid + "]").build();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(2, tf);

        try {
            bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new OpenHandler(this));

            bootstrap.connect(socketAddr).sync();

            active.await();
        } catch (Exception e) {
            LOG.warn(String.format("Could not connect to StreamListener %s", uuid.toString()));
        }
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        LOG.info("Starting olap stream listener " + this);
        ctx.writeAndFlush(new StreamProtocol.InitOlapStream(uuid)).sync();
        active.countDown();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        lock.lock();
        try {
            if (msg instanceof StreamProtocol.PauseStream) {
                if (clientConsuming) {
                    LOG.trace("Client is overloaded, asked for a pause: " + this);
                    clientConsuming = false;
                }
            } else if (msg instanceof StreamProtocol.ContinueStream) {
                if (!clientConsuming) {
                    LOG.trace("Client is resuming to consume: " + this);
                    clientConsuming = true;
                    consumingCondition.signalAll();
                }
            } else {
                LOG.warn(String.format("Unknown message type %s", msg.getClass()));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "OlapStreamListener{" +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", uuid=" + uuid.toString() +
                ", clientConsuming=" + clientConsuming +
                '}';
    }
}
