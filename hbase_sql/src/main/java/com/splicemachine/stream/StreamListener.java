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
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
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
import java.util.*;
import java.util.concurrent.*;


/**
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class StreamListener<T> extends ChannelInboundHandlerAdapter implements Iterator<T> {
    private static final Logger LOG = Logger.getLogger(StreamListener.class);
    private static final Object SENTINEL = new Object();
    private final int queueSize;
    private final int batchSize;
    private final UUID uuid;
    private long limit;
    private long offset;

    private Map<Channel, Integer> partitionMap = new ConcurrentHashMap<>();
    private Map<Integer, Channel> channelMap = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, PartitionState> partitionStateMap = new ConcurrentHashMap<>();

    private T currentResult;
    private int currentQueue = -1;
    // There's at least one partition, this will be updated when we get a connection
    private volatile long numPartitions = 1;
    private List<AutoCloseable> closeables = new ArrayList<>();

    public StreamListener() {
        this(-1, 0, 2, 512);
    }

    public StreamListener(long limit, long offset) {
        this(limit, offset, 2, 512);
    }

    public StreamListener(long limit, long offset, int batches, int batchSize) {
        this.offset = offset;
        this.limit = limit;
        this.batchSize = batchSize;
        this.queueSize = batches*batchSize;
        // start with this to force a channel advancement
        PartitionState first = new PartitionState(0);
        first.messages.add(SENTINEL);
        first.initialized = true;
        this.partitionStateMap.put(-1, first);
        this.uuid = UUID.randomUUID();
    }

    public Iterator<T> getIterator() {
        // Initialize first partition
        partitionStateMap.putIfAbsent(0, new PartitionState(queueSize));
        // This will block until some data is available
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

        if (msg instanceof StreamProtocol.RequestClose) {
            Integer partition = partitionMap.get(channel);
            PartitionState state = partitionStateMap.get(partition);
            // We can't block here, we negotiate throughput with the server to guarantee it
            state.messages.add(SENTINEL);
            try {
                // Let server know it can close the connection
                ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
                ctx.close().sync();
            } catch (InterruptedException e) {
                LOG.error("While closing channel", e);
                throw new RuntimeException(e);
            }
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            try {
                ctx.close().sync();
                channelMap.remove(partitionMap.get(channel));
            } catch (InterruptedException e) {
                LOG.error("While closing channel", e);
                throw new RuntimeException(e);
            }
        } else {
            // Data or StreamProtocol.Skipped
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
                // We take a message first to make sure we have a connection
                Object msg = state.messages.take();
                if (!state.initialized && (offset > 0 || limit > 0)) {
                    LOG.trace("Sending skip " + limit + ", " + offset);
                    // Limit on the server counts from its first element, we have to add offset to it
                    long serverLimit = limit > 0 ? limit + offset : -1;
                    channelMap.get(currentQueue).writeAndFlush(new StreamProtocol.Skip(serverLimit, offset));
                }
                state.initialized = true;
                if (msg == SENTINEL) {
                    // This queue is finished, start reading from the next queue
                    LOG.trace("Moving queues");

                    clearCurrentQueue();

                    currentQueue++;
                    if (currentQueue >= numPartitions) {
                        // finished
                        LOG.trace("End of stream");
                        currentResult = null;
                        close();
                        return;
                    }

                    // Set the partitionState so we can block on the queue in case the connection hasn't opened yet
                    partitionStateMap.putIfAbsent(currentQueue, new PartitionState(queueSize));
                } else {
                    if (msg instanceof StreamProtocol.Skipped) {
                        StreamProtocol.Skipped skipped = (StreamProtocol.Skipped) msg;
                        offset -= skipped.skipped;
                    } else if (offset > 0) {
                        // We still have to ignore 'offset' messages
                        offset--;
                        state.consumed++;
                    } else {
                        // We are returning a message
                        next = (T) msg;
                        state.consumed++;
                        // Check the limit
                        if (limit > 0) {
                            limit--;
                            if (limit == 0) {
                                stopAllStreams();
                            }
                        }
                    }

                    if (state.consumed > batchSize) {
                        LOG.trace("Writing CONT");
                        channelMap.get(currentQueue).writeAndFlush(new StreamProtocol.Continue());
                        state.consumed -= batchSize;
                    }
                }
            }
            currentResult = next;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void clearCurrentQueue() {
        partitionStateMap.remove(currentQueue);
        Channel removedChannel = channelMap.remove(currentQueue);
        if (removedChannel != null)
            partitionMap.remove(removedChannel);
    }

    private void stopAllStreams() throws InterruptedException {
        LOG.trace("Stopping all streams");
        for (Channel channel : channelMap.values()) {
            channel.writeAndFlush(new StreamProtocol.Skip(0, 0));
            channel.writeAndFlush(new StreamProtocol.RequestClose());
        }
        // create fake queue with finish message so the next call to next() returns null
        currentQueue = (int) numPartitions + 1;
        PartitionState ps = new PartitionState(0);
        ps.messages.add(SENTINEL);
        partitionStateMap.putIfAbsent(currentQueue, ps);
    }

    private void close() throws InterruptedException {
        for (Channel channel : channelMap.values()) {
            channel.closeFuture().sync();
        }
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void accept(ChannelHandlerContext ctx, int numPartitions, int partition) {
        LOG.info(String.format("Accepting connection from partition %d out of %d", partition, numPartitions));
        Channel channel = ctx.channel();
        this.numPartitions = numPartitions;

        partitionMap.put(channel, partition);
        partitionStateMap.putIfAbsent(partition, new PartitionState(queueSize));
        channelMap.put(partition, channel);

        ctx.pipeline().addLast(this);
    }

    public UUID getUuid() {
        return uuid;
    }

    public void addCloseable(AutoCloseable autoCloseable) {
        this.closeables.add(autoCloseable);
    }
}

class PartitionState {
    ArrayBlockingQueue messages;
    long consumed;
    boolean initialized;

    PartitionState(int queueSize) {
        messages = new ArrayBlockingQueue(queueSize + 2);  // Two more to account for the SENTINEL & Skipped
    }
}