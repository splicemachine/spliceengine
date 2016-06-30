package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.google.common.net.HostAndPort;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.pipeline.Exceptions;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.sparkproject.guava.util.concurrent.ListenableFuture;
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
    private static final Object FAILURE = new Object();
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
    private volatile boolean closed;
    private volatile Throwable failure;
    private volatile boolean canBlock = true;
    private volatile boolean stopped = false;

    StreamListener() {
        this(-1, 0);
    }

    StreamListener(long limit, long offset) {
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
        PartitionState ps = partitionStateMap.putIfAbsent(0, new PartitionState(queueSize));
        if (failure != null) {
            ps.messages.add(FAILURE);
        }
        // This will block until some data is available
        advance();
        return this;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        LOG.error("Exception caught", cause);
        failed(cause);
        ctx.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel channel = ctx.channel();

        if (msg instanceof StreamProtocol.RequestClose) {
            Integer partition = partitionMap.get(channel);
            PartitionState state = partitionStateMap.get(partition);
            // We can't block here, we negotiate throughput with the server to guarantee it
            state.messages.add(SENTINEL);
            // Let server know it can close the connection
            ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
            ctx.close().sync();
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            ctx.close().sync();
            channelMap.remove(partitionMap.get(channel));
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
        if (failure != null) {
            // The remote job failed, raise exception to caller
            Exceptions.throwAsRuntime(Exceptions.parseException(failure));
        }

        return currentResult != null;
    }

    @Override
    public T next() {
        T result = currentResult;
        advance();
        if (failure != null) {
            // The remote job failed, raise exception to caller
            Exceptions.throwAsRuntime(Exceptions.parseException(failure));
        }
        return result;
    }

    private void advance() {
        T next = null;
        try {
            while (next == null) {
                PartitionState state = partitionStateMap.get(currentQueue);
                // We take a message first to make sure we have a connection
                Object msg = canBlock ? state.messages.take() : state.messages.remove();
                if (!state.initialized && (offset > 0 || limit > 0)) {
                    if (LOG.isTraceEnabled())
                        LOG.trace("Sending skip " + limit + ", " + offset);
                    // Limit on the server counts from its first element, we have to add offset to it
                    long serverLimit = limit > 0 ? limit + offset : -1;
                    channelMap.get(currentQueue).writeAndFlush(new StreamProtocol.Skip(serverLimit, offset));
                }
                state.initialized = true;
                if (msg == FAILURE) {
                    // The olap job failed, return
                    currentResult = null;
                    return;
                } else if (msg == SENTINEL) {
                    // This queue is finished, start reading from the next queue
                    LOG.trace("Moving queues");

                    clearCurrentQueue();

                    currentQueue++;
                    if (currentQueue >= numPartitions) {
                        // finished
                        if (LOG.isTraceEnabled())
                            LOG.trace("End of stream");
                        currentResult = null;
                        close();
                        return;
                    }

                    // Set the partitionState so we can block on the queue in case the connection hasn't opened yet
                    PartitionState ps = partitionStateMap.putIfAbsent(currentQueue, new PartitionState(queueSize));
                    if (failure != null) {
                        ps.messages.add(FAILURE);
                    }
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
                        if (LOG.isTraceEnabled())
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

    /**
     * This method stops all streams when the iterator is finished.
     *
     * It is synchronized as is the accept() method because we want to notify channels only once that we are no longer
     * interested in more data.
     */
    public synchronized void stopAllStreams() {
        if (LOG.isTraceEnabled())
            LOG.trace("Stopping all streams");
        if (closed) {
            // do nothing
            return;
        }
        stopped = true;
        // If a new channel has been added concurrently, it's either visible on the channelMap, so we are going to close it,
        // or it has already seen the stopped flag, so it's been closed in accept()
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


    /**
     * This method accepts new connections from the StreamListenerServer
     *
     * It is synchronized as is the stopAllStreams() method because we want to notify channels only once that we are no longer
     * interested in more data.
     */
    public synchronized void accept(ChannelHandlerContext ctx, int numPartitions, int partition) {
        LOG.info(String.format("Accepting connection from partition %d out of %d", partition, numPartitions));
        Channel channel = ctx.channel();
        this.numPartitions = numPartitions;

        partitionMap.put(channel, partition);
        PartitionState ps = partitionStateMap.putIfAbsent(partition, new PartitionState(queueSize));
        if (failure != null) {
            ps.messages.add(FAILURE);
        }
        channelMap.put(partition, channel);
        if (stopped) {
            // we are already stopped, ask this stream to close
            channel.writeAndFlush(new StreamProtocol.Skip(0, 0));
            channel.writeAndFlush(new StreamProtocol.RequestClose());
        }

        ctx.pipeline().addLast(this);
    }

    private void close() {
        if (closed) {
            // do nothing
            return;
        }
        closed = true;
        for (Channel channel : channelMap.values()) {
            channel.closeFuture(); // don't wait synchronously, no need
        }
        Exception lastException = null;
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                LOG.error(e);
                lastException = e;
            }
        }
        if (lastException != null) {
            throw new RuntimeException(lastException);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public UUID getUuid() {
        return uuid;
    }

    public void addCloseable(AutoCloseable autoCloseable) {
        this.closeables.add(autoCloseable);
    }

    public void completed(OlapResult result) {
        // the olap job completed, we shouldn't block anymore
        canBlock = false;
    }

    public void failed(Throwable e) {
        LOG.error("StreamListener failed", e);
        failure = e;

        // Unblock iterator
        for (PartitionState state : partitionStateMap.values()) {
            if (state != null) {
                state.messages.add(FAILURE);
            }
        }
    }
}

class PartitionState {
    ArrayBlockingQueue messages;
    long consumed;
    boolean initialized;

    PartitionState(int queueSize) {
        messages = new ArrayBlockingQueue(queueSize + 3);  // Extra to account for out of band messages
    }
}