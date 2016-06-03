package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.serializer.KryoRegistrator;
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
import org.sparkproject.io.netty.bootstrap.Bootstrap;
import org.sparkproject.io.netty.channel.*;
import org.sparkproject.io.netty.channel.nio.NioEventLoopGroup;
import org.sparkproject.io.netty.channel.socket.SocketChannel;
import org.sparkproject.io.netty.channel.socket.nio.NioSocketChannel;
import org.sparkproject.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

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
    private volatile long sent = 0;
    private Integer partition;
    private Iterator<T> locatedRowIterator;
    private ExecutorService executor;

    // Serialization
    public ResultStreamer() {
    }

    public ResultStreamer(String host, int port, int numPartitions) {
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ctx.write((long) numPartitions);
        ctx.writeAndFlush(partition);
        this.executor = Executors.newFixedThreadPool(1);
        this.executor.submit(new Runnable() {
            @Override
            public void run() {
                LOG.trace("Written init");

                while (locatedRowIterator.hasNext()) {
                    if (sent % 512 == 0) {
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
                    T lr = locatedRowIterator.next();
                    ctx.write(lr);

//                    LOG.trace("Written " + lr);
                    sent++;
                }
                LOG.trace("Written data");

                ctx.writeAndFlush(new String("FIN"));
                LOG.trace("Written FIN");
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOG.trace("Received message " + msg);

        if (msg instanceof String) {
            if ("CONT".equals(msg)) {
                LOG.trace("Releasing permit " + cont);
                cont.release();
                LOG.trace("Released permit " + cont);
            } else {
                try {
                    ctx.close().sync();
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
                LOG.trace("Closed");
            }
        }
    }

    @Override
    public Iterator<Object> call(Integer partition, Iterator<T> locatedRowIterator) throws Exception {
        InetSocketAddress socketAddr=new InetSocketAddress(host,port);
        this.partition = partition;
        this.locatedRowIterator = locatedRowIterator;

        Bootstrap bootstrap;
        EventLoopGroup workerGroup = new NioEventLoopGroup(4);
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

            return Collections.emptyIterator();
        } finally {
            workerGroup.shutdownGracefully();
//            pool.returnInstance(kryo);
        }
    }
}
