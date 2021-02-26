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

import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.pipeline.Exceptions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * This class handles connections from Spark tasks streaming data to the query client. One connection is created from
 * each task, it handles failures and recovery in case the task is retried.
 *
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class StreamListener<T> extends ChannelInboundHandlerAdapter implements Iterator<T> {
    private static final Logger LOG = Logger.getLogger(StreamListener.class);
    private static final Object SENTINEL = new Object();
    private static final Object FAILURE = new Object();
    private static final Object RETRY = new Object();
    private final int queueSize;
    private final int batchSize;
    private final UUID uuid;
    private long limit;
    private long offset;

    private Map<Channel, PartitionState> partitionMap = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, PartitionState> partitionStateMap = new ConcurrentHashMap<>();

    private T currentResult;
    private int currentQueue = -1;
    // There's at least one partition, this will be updated when we get a connection
    private volatile long numPartitions = 1;
    private final List<AutoCloseable> closeables = new ArrayList<>();
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
        PartitionState first = new PartitionState(0, 0);
        first.messages.add(SENTINEL);
        first.initialized = true;
        this.partitionStateMap.put(-1, first);
        this.uuid = UUID.randomUUID();
    }

    public Iterator<T> getIterator() {
        // Initialize first partition
        PartitionState ps = partitionStateMap.computeIfAbsent(0, k -> new PartitionState(1, queueSize));
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

        PartitionState state = partitionMap.get(channel);
        if (state == null) {
            // Removed channel, possibly retried task, ignore
            LOG.warn("Received message from removed channel");
            return;
        }
        if (msg instanceof StreamProtocol.RequestClose) {
            // We can't block here, we negotiate throughput with the server to guarantee it
            state.messages.add(SENTINEL);
            // Let server know it can close the connection
            ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
            ctx.close().sync();
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            ctx.close().sync();
            partitionMap.remove(channel);
        } else {
            // Data or StreamProtocol.Skipped
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
                if (msg == FAILURE) {
                    // The olap job failed, return right away
                    currentResult = null;
                    return;
                }
                if (!state.initialized && (offset > 0 || limit > 0)) {
                    if (LOG.isTraceEnabled())
                        LOG.trace("Sending skip " + limit + ", " + offset);
                    // Limit on the server counts from its first element, we have to add offset to it
                    long serverLimit = limit > 0 ? limit + offset : -1;
                    state.channel.writeAndFlush(new StreamProtocol.Skip(serverLimit, offset));
                }
                state.initialized = true;
                if (msg == RETRY) {
                    // There was a retried task
                    long currentRead = state.readTotal;
                    long currentOffset = offset + currentRead;
                    long serverLimit = limit > 0 ? limit + currentOffset : -1;

                    // Skip all records already read from the previous run of the task
                    state.next.channel.writeAndFlush(new StreamProtocol.Skip(serverLimit, currentOffset));
                    state.next.initialized = true;
                    state.messages.clear();
                    offset = currentOffset;

                    // Update maps with the new state/channel
                    partitionStateMap.put(currentQueue, state.next);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Retried task, currentRead " + currentRead + " offset " + offset +
                                " currentOffset " + currentOffset + " serverLimit " + serverLimit + " state " + state);
                    }
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
                    PartitionState ps = partitionStateMap.computeIfAbsent(currentQueue, k -> new PartitionState(currentQueue, queueSize));
                    if (failure != null) {
                        ps.messages.add(FAILURE);
                    }
                } else {
                    if (msg instanceof StreamProtocol.Skipped) {
                        StreamProtocol.Skipped skipped = (StreamProtocol.Skipped) msg;
                        offset -= skipped.skipped;
                        state.readTotal += skipped.skipped;
                    } else if (offset > 0) {
                        // We still have to ignore 'offset' messages
                        offset--;
                        state.consumed++;
                        state.readTotal++;
                    } else {
                        // We are returning a message
                        next = (T) msg;
                        state.consumed++;
                        state.readTotal++;
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
                        state.channel.writeAndFlush(new StreamProtocol.Continue());
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
        PartitionState ps = partitionStateMap.remove(currentQueue);
        if (ps != null && ps.channel != null)
            partitionMap.remove(ps.channel);
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
        // If a new channel has been added concurrently, it's either visible on the partitionMap, so we are going to close it,
        // or it has already seen the stopped flag, so it's been closed in accept()
        for (Channel channel : partitionMap.keySet()) {
            channel.writeAndFlush(new StreamProtocol.RequestClose());
        }
        // create fake queue with finish message so the next call to next() returns null
        currentQueue = (int) numPartitions + 1;
        PartitionState ps = new PartitionState(currentQueue, 0);
        ps.messages.add(SENTINEL);
        partitionStateMap.putIfAbsent(currentQueue, ps);
        ps = partitionStateMap.get(currentQueue);
        if (ps != null)
            ps.messages.add(FAILURE); // just in case we are blocked in advance()
        close();
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

        PartitionState ps = new PartitionState(partition, queueSize);
        PartitionState old = partitionStateMap.putIfAbsent(partition, ps);
        ps = old != null ? old : ps;

        if (failure != null) {
            ps.messages.add(FAILURE);
        }
        Channel previousChannel = ps.channel;
        if (previousChannel != null) {
            LOG.info("Received connection from retried task, current state " + ps);
            PartitionState nextState = new PartitionState(partition, queueSize);
            nextState.channel = channel;
            ps.next = nextState;
            partitionMap.put(channel, ps.next);
            partitionMap.remove(ps.channel); // don't accept more messages from this channel
            // this is a new connection from a retried task
            ps.messages.add(RETRY);
        } else {
            partitionMap.put(channel, ps);
            ps.channel = channel;
        }
        if (stopped) {
            // we are already stopped, ask this stream to close
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
        for (Channel channel : partitionMap.keySet()) {
            channel.closeFuture(); // don't wait synchronously, no need
        }
        Exception lastException = null;
        synchronized (closeables) {
            for (AutoCloseable c : closeables) {
                try {
                    c.close();
                } catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                    lastException = e;
                }
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
        synchronized (closeables) {
            if (closed) {
                try {
                    autoCloseable.close();
                } catch (Exception e) {
                    LOG.error("Error while closing resource", e);
                }
            } else {
                this.closeables.add(autoCloseable);
            }
        }
    }

    public void completed(OlapResult result) {
        // the olap job completed, we shouldn't block anymore
        canBlock = false;
        if  (result instanceof QueryResult) {
            QueryResult qr = (QueryResult) result;
            if (qr.numPartitions == 0) {
                // This query was empty, unblock iterator
                this.numPartitions = 0;
                for (PartitionState state : partitionStateMap.values()) {
                    if (state != null) {
                        state.messages.add(SENTINEL);
                    }
                }
            }
        }
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
    int partition;
    Channel channel;
    ArrayBlockingQueue<Object> messages;
    long consumed;
    long readTotal;
    boolean initialized;
    volatile PartitionState next = null; // used when a task is retried after a failure

    PartitionState(int partition, int queueSize) {
        this.partition = partition;
        this.messages = new ArrayBlockingQueue<>(queueSize + 4);  // Extra to account for out of band messages
    }

    @Override
    public String toString() {
        return "PartitionState{" +
                "partition=" + partition +
                ", channel=" + channel +
                ", messages=" + messages.size() +
                ", consumed=" + consumed +
                ", initialized=" + initialized +
                ", next=" + next +
                '}';
    }
}
