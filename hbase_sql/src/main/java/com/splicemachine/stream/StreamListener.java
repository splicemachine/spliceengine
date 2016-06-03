package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.google.common.net.HostAndPort;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.pipeline.Exceptions;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.sparkproject.io.netty.bootstrap.ServerBootstrap;
import org.sparkproject.io.netty.channel.*;
import org.sparkproject.io.netty.channel.nio.NioEventLoopGroup;
import org.sparkproject.io.netty.channel.socket.SocketChannel;
import org.sparkproject.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.sparkproject.io.netty.handler.logging.LogLevel;
import org.sparkproject.io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;


/**
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class StreamListener<T> extends ChannelInboundHandlerAdapter implements Iterator<T> {
    private static final Logger LOG = Logger.getLogger(StreamListener.class);
    private static final KryoRegistrator registry = new SpliceSparkKryoRegistrator();
    private static final Object SENTINEL = new Object();
    private static final StreamProtocol.Continue CONTINUE = new StreamProtocol.Continue();
    private static final StreamProtocol.Close CLOSE = new StreamProtocol.Close();

    private Channel serverChannel;
    private Map<Channel, Integer> partitionMap = new ConcurrentHashMap<>();
    private Map<Integer, Channel> channelMap = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, PartitionState> partitionStateMap = new ConcurrentHashMap<>();
    private boolean initialized = false;

    private T currentResult;
    private int currentQueue = 0;
    private long numPartitions;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;

    public StreamListener() {
    }

    public HostAndPort start() throws StandardException {

        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            Kryo encoder =  new Kryo(new DefaultClassResolver(),new MapReferenceResolver());
                            registry.registerClasses(encoder);
                            Kryo decoder =  new Kryo(new DefaultClassResolver(),new MapReferenceResolver());
                            registry.registerClasses(decoder);
                            ch.pipeline().addLast(
                                    new KryoEncoder(encoder),
                                    new KryoDecoder(decoder),
                                    StreamListener.this);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(0).sync();

            this.serverChannel = f.channel();
            InetSocketAddress socketAddress = (InetSocketAddress)this.serverChannel.localAddress();
            String host = InetAddress.getLocalHost().getHostName();
            int port = socketAddress.getPort();
            return HostAndPort.fromParts(host, port);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
//        } finally {
//            workerGroup.shutdownGracefully();
//            bossGroup.shutdownGracefully();
        }

    }

    public Iterator<T> getIterator() {
        int sleep = 100;
        while (!initialized) {

            LOG.warn("Waiting for initilization on iterator");
            try {
                Thread.sleep(sleep);
                sleep = Math.min(2000, sleep*2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        LOG.warn("Got iterator!");
        // Initialize first partition
        partitionStateMap.putIfAbsent(0, new PartitionState());
        advance();
        return this;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        LOG.error("Exception caught", cause);
        cause.printStackTrace();
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Channel channel = ctx.channel();

        if (msg instanceof StreamProtocol.Init) {
            StreamProtocol.Init init = (StreamProtocol.Init) msg;
            LOG.trace("Received " + msg + " from " + channel);
            synchronized (this) {
                // Init global state for receiver
                if (!initialized) {
                    numPartitions = init.numPartitions;
                    initialized = true;
                }
            }
            partitionMap.put(channel, init.partition);
            partitionStateMap.putIfAbsent(init.partition, new PartitionState());
            channelMap.put(init.partition, channel);
        } else if (msg instanceof StreamProtocol.Close) {
            Integer partition = partitionMap.get(channel);
            PartitionState state = partitionStateMap.get(partition);
            // We can't block here, we negotiate throughput with the server to guarantee it
            state.messages.add(SENTINEL);
            try {
                // Let server know it can close the connection
                ctx.writeAndFlush(CLOSE);
                ctx.close().sync();
            } catch (InterruptedException e) {
                LOG.error("While closing channel", e);
                throw new RuntimeException(e);
            }
        } else {
            // Data
            Integer partition = partitionMap.get(channel);
            PartitionState state = partitionStateMap.get(partition);
            // We can't block here, we negotiate throughput with the server to guarantee it
            state.messages.add(msg);
        }
    }

    @Override
    public boolean hasNext() {
        return currentResult != null;
    }

    @Override
    public T next() {
        T result = currentResult;
        advance();
        return result;
    }

    private void advance() {
        T next = null;
        try {
            while (next == null) {
                PartitionState state = partitionStateMap.get(currentQueue);
                Object msg = state.messages.take();
                if (msg == SENTINEL) {
                    LOG.trace("Moving queues");
                    currentQueue++;
                    if (currentQueue >= numPartitions) {
                        // finished
                        LOG.trace("End of stream");
                        currentResult = null;
                        close();
                        return;
                    } else {
                        partitionStateMap.putIfAbsent(currentQueue, new PartitionState());
                    }
                } else {
                    next = (T) msg;
                    state.returned++;
                    if (state.returned % 512 == 0) {
                        LOG.trace("Writing CONT");
                        channelMap.get(currentQueue).writeAndFlush(CONTINUE);
                    }
                }
            }
            currentResult = next;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void close() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

class PartitionState {
    ArrayBlockingQueue messages = new ArrayBlockingQueue(1025); // One more to account for the SENTINEL
    long returned;

}