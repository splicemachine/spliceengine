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


import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.stream.handlers.OpenHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskKilledException;
import org.apache.spark.api.java.function.Function2;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class ResultStreamer<T> extends ChannelInboundHandlerAdapter implements Function2<Integer, Iterator<T>, Iterator<String>>, Serializable, Externalizable {
    private static final long serialVersionUID = -6509694203842227972L;
    private static final Logger LOG = Logger.getLogger(ResultStreamer.class);

    private OperationContext<?> context;
    private UUID uuid;
    private int batchSize;
    private int numPartitions;
    private String host;
    private int port;
    private Semaphore permits;
    private volatile long offset = 0;
    private volatile long limit = Long.MAX_VALUE;
    private Integer partition;
    private transient Iterator<T> locatedRowIterator;
    private volatile Future<Long> future;
    private transient NioEventLoopGroup workerGroup;
    private transient CountDownLatch active;
    private int batches;
    private volatile TaskContext taskContext;
    private transient CountDownLatch runningOnClient;

    // Serialization
    public ResultStreamer() {
    }

    public ResultStreamer(OperationContext<?> context, UUID uuid, String host, int port, int numPartitions, int batches, int batchSize) {
        this.context = context;
        this.uuid = uuid;
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
        this.batches = batches;
        this.batchSize = batchSize;
        this.permits = new Semaphore(batches - 1); // we start with one permit taken
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        LOG.trace("Starting result streamer " + this);
        // Write init data right away
        ctx.writeAndFlush(new StreamProtocol.Init(uuid, numPartitions, partition));
        // Subsequent writes are from a separate thread, so we don't block this one
        this.future = this.workerGroup.submit(new Callable<Long>() {
            private long consumed;
            private long sent;
            private int currentBatch;

            @Override
            public Long call() throws InterruptedException {
                org.apache.spark.TaskContext$.MODULE$.setTaskContext(taskContext);
                boolean prepared = false;
                ActivationHolder ah = null;
                if (context != null) {
                    ah = context.getActivationHolder();
                    ah.reinitialize(null, false);
                    prepared = true;
                }
                try {
                    while (locatedRowIterator.hasNext()) {
                        T lr = locatedRowIterator.next();
                        consumed++;


                        ctx.write(lr, ctx.voidPromise());
                        currentBatch++;
                        sent++;

                        flushAndGetPermit();

                        if (checkLimit()) {
                            return consumed;
                        }

                        consumeOffset();
                    }
                    // Data has been written, request close
                    ctx.writeAndFlush(new StreamProtocol.RequestClose());

                    return consumed;
                } finally {
                    if (prepared)
                        ah.close();
                }
            }

            /**
             * If the current batch exceeds the batch size, flush the connection and take a new permit, blocking if the client
             * hasn't had time yet to process previous messages
             */
            private void flushAndGetPermit() throws InterruptedException {
                if (currentBatch >= batchSize) {
                    ctx.flush();
                    currentBatch = 0;
                    permits.acquire();
                    if (taskContext.isInterrupted())
                        throw new TaskKilledException();
                }
            }

            /**
             * If the client hast told us to ignore up to 'offset' messages, consume them here. The client request can
             * arrive after we've already sent some messages.
             */
            private void consumeOffset() {
                if (consumed < offset) {
                    long count = 0;
                    while (locatedRowIterator.hasNext() && consumed < offset) {
                        locatedRowIterator.next();
                        count++;
                        consumed++;
                    }
                    ctx.writeAndFlush(new StreamProtocol.Skipped(count));
                }
            }

            /**
             * If the client told us to send no more than 'limit' messages, check it here
             * @return true if there's a limit and we reached it, false otherwise
             */
            private boolean checkLimit() {
                if (consumed > limit) {
                    ctx.flush();
                    if (LOG.isTraceEnabled())
                        LOG.trace("Reached limit, stopping. consumed " + consumed + " sent " + sent + " limit " + limit);
                    return true;
                }
                return false;
            }
        });
        active.countDown();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof StreamProtocol.Continue) {
            permits.release();
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            ctx.close().sync();
            this.runningOnClient.countDown();
        } else if (msg instanceof StreamProtocol.RequestClose) {
            this.runningOnClient.countDown();
            limit = 0; // If they want to close they don't need more data
            permits.release();
            // wait for the writing thread to finish
            future.get();
            ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
            ctx.close().sync();
        } else if (msg instanceof StreamProtocol.Skip) {
            StreamProtocol.Skip skip = (StreamProtocol.Skip) msg;
            offset = skip.offset;
            if (skip.limit >= 0) {
                limit = skip.limit;
            }
        }
    }

    /**
     * Throttling mechanism explanation: The throttling mechanism we have currently, see StreamableRDD.submit(), is to
     * run 2 concurrent jobs at a time on the spark side, each sending rows from 2 partitions to the region server.
     * If the buffer for a particular partition is full, it would cause back-pressure,
     * and prevent Spark from sending more rows for that partition.
     * However, if the number of rows in the partition is smaller than the buffer size, then there is another mechanism
     * to hold Spark from sending all the rows for that partition to the region server is implemented via CountDownLatch
     * runningOnClient: ResultStreamer keeps the connection to StreamListener opened until
     * StreamProtocol.ConfirmClose/RequestClose messages come (runningOnClient awaits) and does not return from
     * the method call until the partition will be processed on StreamListener.
     */
    @Override
    public Iterator<String> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        InetSocketAddress socketAddr=new InetSocketAddress(host,port);
        this.partition = partition;
        this.locatedRowIterator = locatedRowIterator;
        this.active = new CountDownLatch(1);
        this.runningOnClient = new CountDownLatch(1);
        taskContext = TaskContext.get();
        Bootstrap bootstrap;
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ResultStreamer-"+host+":"+port+"["+partition+"]").build();
        this.workerGroup = new NioEventLoopGroup(2, tf);

        try {

            bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
            bootstrap.handler(new OpenHandler(this));


            ChannelFuture futureConnect = bootstrap.connect(socketAddr).sync();

            active.await();
            long consumed;
            while (true) {
                try {
                    consumed = future.get(10, TimeUnit.SECONDS);
                    break;
                } catch (TimeoutException e) {
                    if (taskContext.isInterrupted()) {
                        permits.release();
                        throw new TaskKilledException();
                    }
                }
            }
            try {
                this.runningOnClient.await(2, TimeUnit.MINUTES);
            } catch (Exception e) {
            }
            if (this.runningOnClient.getCount() > 0) {
                this.runningOnClient.countDown();
                futureConnect.channel().writeAndFlush(new StreamProtocol.RequestClose());
                futureConnect.channel().closeFuture();
            } else {
                futureConnect.channel().closeFuture().sync();
            }

            String result;
            if (consumed >= limit) {
                // We reached the limit, stop processing partitions
                result = "STOP";
            } else {
                // We didn't reach the limit, continue executing more partitions
                result = "CONTINUE";
            }
            return Arrays.asList(result).iterator();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public String toString() {
        return "ResultStreamer{" +
                "batchSize=" + batchSize +
                ", numPartitions=" + numPartitions +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", permits=" + permits +
                ", offset=" + offset +
                ", limit=" + limit +
                ", partition=" + partition +
                ", batches=" + batches +
                ", running=" + runningOnClient +
                '}';
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(context);
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
        out.writeUTF(host);
        out.writeInt(port);
        out.writeInt(numPartitions);
        out.writeInt(batches);
        out.writeInt(batchSize);
        out.writeObject(permits); // WTF is this?
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        context = (OperationContext) in.readObject();
        uuid = new UUID(in.readLong(),in.readLong());
        host = in.readUTF();
        port = in.readInt();
        numPartitions = in.readInt();
        batches = in.readInt();
        batchSize = in.readInt();
        permits = (Semaphore) in.readObject();
    }
}
