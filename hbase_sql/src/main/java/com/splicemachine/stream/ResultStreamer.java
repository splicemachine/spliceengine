package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
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
import java.util.concurrent.*;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class ResultStreamer<T> extends ChannelInboundHandlerAdapter implements Function2<Integer, Iterator<T>, Iterator<Object>>, Serializable {
    private static final Logger LOG = Logger.getLogger(ResultStreamer.class);

    private static final KryoRegistrator registry = new SpliceSparkKryoRegistrator();
    private int numPartitions;
    private String host;
    private int port;
    private Semaphore cont = new Semaphore(2);
    private volatile long consumed = 0;
    private volatile long sent = 0;
    private volatile long offset = 0;
    private volatile long limit = Long.MAX_VALUE;
    private Integer partition;
    private Iterator<T> locatedRowIterator;
    private volatile Future<String> future;
    private NioEventLoopGroup workerGroup;
    private int batchSize;

    // Serialization
    public ResultStreamer() {
    }

    public ResultStreamer(String host, int port, int numPartitions, int batchSize) {
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
        this.batchSize = batchSize;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(new StreamProtocol.Init(numPartitions, partition));
        LOG.trace("Written init");
        this.future = this.workerGroup.submit(new Callable<String>() {
            @Override
            public String call() {
                while (locatedRowIterator.hasNext()) {
                    if (consumed < offset) {
                        long count = 0;
                        while (locatedRowIterator.hasNext() && consumed < offset) {
                            locatedRowIterator.next();
                            count++;
                            consumed++;
                        }
                        ctx.writeAndFlush(new StreamProtocol.Skipped(count));
                        if (!locatedRowIterator.hasNext())
                            break;
                    }
                    T lr = locatedRowIterator.next();
                    consumed++;
                    ctx.write(lr, ctx.voidPromise());
                    sent++;
                    if ((sent - 1) % batchSize == 0) {
                        LOG.trace("Acquiring permit " + cont.toString() + " sent " + sent);
                        ctx.flush();
                        try {
                            cont.acquire();
                        } catch (InterruptedException e) {
                            LOG.error(e);
                            throw new RuntimeException(e);
                        }
                        LOG.trace("Acquired permit " + cont.toString()+ " sent " + sent);
                    }

                    if (consumed > limit) {
                        ctx.flush();
                        LOG.trace("Reached limit, stopping consumed " + consumed + " sent " + sent + " limit " + limit);
                        return "STOP";
                    }
                }
                LOG.trace("Written data");

                ctx.writeAndFlush(new StreamProtocol.RequestClose());

                LOG.trace("Written FIN");
                return "CONTINUE";
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOG.trace("Received message " + msg);

        if (msg instanceof StreamProtocol.Continue) {
            LOG.trace("Releasing permit " + cont);
            cont.release();
            LOG.trace("Released permit " + cont);
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            try {
                ctx.close().sync();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
            LOG.trace("Closed");
        } else if (msg instanceof StreamProtocol.RequestClose) {
            cont.release();
            try {
                // wait for the writing thread to finish
                future.get();
                ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
                ctx.close().sync();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
            LOG.trace("Closed");
        } else if (msg instanceof StreamProtocol.Skip) {
            StreamProtocol.Skip skip = (StreamProtocol.Skip) msg;
            offset = skip.offset;
            if (skip.limit >= 0) {
                limit = skip.limit;
            }
        }
    }

    @Override
    public Iterator<Object> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        InetSocketAddress socketAddr=new InetSocketAddress(host,port);
        this.partition = partition;
        this.locatedRowIterator = locatedRowIterator;

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
            futureConnect.channel().closeFuture().sync();

            String result = future.get();
            if (consumed >= limit) {
                result = "STOP";
            }
            return Arrays.<Object>asList(result).iterator();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
