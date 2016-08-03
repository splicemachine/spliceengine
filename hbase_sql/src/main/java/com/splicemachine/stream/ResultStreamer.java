/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoRegistrator;
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
import org.sparkproject.io.netty.bootstrap.Bootstrap;
import org.sparkproject.io.netty.channel.*;
import org.sparkproject.io.netty.channel.nio.NioEventLoopGroup;
import org.sparkproject.io.netty.channel.socket.SocketChannel;
import org.sparkproject.io.netty.channel.socket.nio.NioSocketChannel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class ResultStreamer<T> extends ChannelInboundHandlerAdapter implements Function2<Integer, Iterator<T>, Iterator<String>>, Serializable {
    private static final Logger LOG = Logger.getLogger(ResultStreamer.class);

    private static final KryoRegistrator registry = new SpliceSparkKryoRegistrator();
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
    private Iterator<T> locatedRowIterator;
    private volatile Future<Long> future;
    private NioEventLoopGroup workerGroup;
    private transient CountDownLatch active;
    private int batches;

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
                boolean prepared = false;
                ActivationHolder ah = null;
                if (context != null) {
                    ah = ((SparkOperationContext) context).getActivationHolder();
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
        } else if (msg instanceof StreamProtocol.RequestClose) {
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

    @Override
    public Iterator<String> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        InetSocketAddress socketAddr=new InetSocketAddress(host,port);
        this.partition = partition;
        this.locatedRowIterator = locatedRowIterator;
        this.active = new CountDownLatch(1);

        Bootstrap bootstrap;
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ResultStreamer-"+host+":"+port+"["+partition+"]").build();
        this.workerGroup = new NioEventLoopGroup(2, tf);
        try {

            bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();
                    Kryo kryo = new Kryo(new DefaultClassResolver(),new MapReferenceResolver());
                    registry.registerClasses(kryo);
                    p.addLast(new KryoEncoder(kryo));
                    kryo = new Kryo(new DefaultClassResolver(),new MapReferenceResolver());
                    registry.registerClasses(kryo);
                    p.addLast(new KryoDecoder(kryo));
                    p.addLast(ResultStreamer.this);
                }
            });


            ChannelFuture futureConnect = bootstrap.connect(socketAddr).sync();

            active.await();
            long consumed = future.get();
            futureConnect.channel().closeFuture().sync();

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
                '}';
    }
}
