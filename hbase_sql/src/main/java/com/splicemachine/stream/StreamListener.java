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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;


/**
 * This class handles connections from Spark tasks streaming data to the query client. One connection is created from
 * each task, it handles failures and recovery in case the task is retried.
 *
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class StreamListener<T> extends ChannelInboundHandlerAdapter implements Iterator<T> {
    private static final Logger LOG = Logger.getLogger(StreamListener.class);
    private static final int PARTITION_BUFFER_FACTOR = 3;
    private static final String SENTINEL = "SENTINEL";
    private static final String FAILURE = "FAILURE";
    private static final String RETRY = "RETRY";
    private final int queueSize;
    private final int batchSize;
    private final int parallelPartitions;
    private final UUID uuid;
    private long limit;
    private long offset;

    private final Map<Channel, PartitionState> partitionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, PartitionState> partitionStateMap = new ConcurrentHashMap<>();

    private T currentResult;
    private int currentQueue = -1;
    // There's at least one partition, this will be updated when we get a connection
    private volatile long numPartitions = 1;
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private volatile boolean closed;
    private volatile Throwable failure;
    private volatile boolean canBlock = true;
    private volatile boolean stopped = false;
    private Channel olapChannel;
    private volatile boolean paused = false;
    private boolean throttleEnabled;
    private volatile Long lastReadTime = null;
    private volatile boolean backupEnabled = false;
    private MessageBackupHandler messageBackupHandler = null;

    StreamListener() {
        this(-1, 0);
    }

    StreamListener(long limit, long offset) {
        this(limit, offset, 2, 512);
    }

    public StreamListener(long limit, long offset, int batches, int batchSize) {
        this(limit, offset, batches, batchSize, StreamableRDD.DEFAULT_PARALLEL_PARTITIONS, true, true);
    }

    public StreamListener(long limit, long offset, int batches, int batchSize, int parallelPartitions,
                          boolean throttleEnabled, boolean fileCacheEnabled) {
        this.offset = offset;
        this.limit = limit;
        this.batchSize = batchSize;
        this.queueSize = batches*batchSize;
        // start with this to force a channel advancement
        PartitionState first = new PartitionState(0, 0);
        addMessage(first, SENTINEL);
        first.initialized = true;
        this.partitionStateMap.put(-1, first);
        this.uuid = UUID.randomUUID();
        this.parallelPartitions = parallelPartitions;
        this.throttleEnabled = throttleEnabled;
        if (fileCacheEnabled)
            this.messageBackupHandler = new MessageBackupHandler(uuid, this);
    }

    public void setOlapChannel(Channel olapChannel) {
        this.olapChannel = olapChannel;
    }

    public Iterator<T> getIterator() {
        // Initialize first partition
        PartitionState ps = partitionStateMap.computeIfAbsent(0, k -> new PartitionState(0, queueSize));
        if (failure != null) {
            addMessage(ps, FAILURE);
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
            addMessage(state, SENTINEL);
            // Let server know it can close the connection
            ctx.writeAndFlush(new StreamProtocol.ConfirmClose());
            ctx.close().sync();
            writeToBackupIfEnabled(state);
        } else if (msg instanceof StreamProtocol.ConfirmClose) {
            ctx.close().sync();
            partitionMap.remove(channel);
        } else if (msg instanceof StreamProtocol.InitOlapStream) {
            //Main handler is in StreamListenerServer, but if InitOlapStream message
            //comes later, than Init message and channel is redirected to the StreamListener
            // already
            LOG.trace("Received " + msg + " from " + channel);
            if (this.olapChannel != null) {
                StreamProtocol.InitOlapStream init = (StreamProtocol.InitOlapStream) msg;
                if (this.uuid.equals(init.uuid)) {
                    this.setOlapChannel(ctx.channel());
                }
            }
        } else {
            // Data or StreamProtocol.Skipped
            // We can't block here, we negotiate throughput with the server to guarantee it
            addMessage(state, msg);
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
        lastReadTime = System.currentTimeMillis();
        return result;
    }

    private void advance() {
        T next = null;
        try {
            while (next == null) {
                PartitionState state = partitionStateMap.get(currentQueue);
                // We take a message first to make sure we have a connection
                ArrayBlockingQueue<Object> messages = getMessages(state);
                Object msg = canBlock ? messages.take() : messages.remove();
                if (msg.equals(FAILURE)) {
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
                if (msg.equals(RETRY)) {
                    // There was a retried task
                    long currentRead = state.readTotal;
                    long currentOffset = offset + currentRead;
                    long serverLimit = limit > 0 ? limit + currentOffset : -1;

                    // Skip all records already read from the previous run of the task
                    state.next.channel.writeAndFlush(new StreamProtocol.Skip(serverLimit, currentOffset));
                    state.next.initialized = true;
                    messages.clear();
                    offset = currentOffset;

                    // Update maps with the new state/channel
                    partitionStateMap.put(currentQueue, state.next);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Retried task, currentRead " + currentRead + " offset " + offset +
                                " currentOffset " + currentOffset + " serverLimit " + serverLimit + " state " + state);
                    }
                } else if (msg.equals(SENTINEL)) {
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
                        addMessage(ps, FAILURE);
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
        manageStreaming();
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
        addMessage(ps, SENTINEL);
        partitionStateMap.putIfAbsent(currentQueue, ps);
        ps = partitionStateMap.get(currentQueue);
        if (ps != null)
            addMessage(ps, FAILURE);
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
            addMessage(ps, FAILURE);
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
            addMessage(ps, RETRY);
        } else {
            partitionMap.put(channel, ps);
            ps.channel = channel;
        }
        if (stopped) {
            // we are already stopped, ask this stream to close
            channel.writeAndFlush(new StreamProtocol.RequestClose());
        }

        ctx.pipeline().addLast(this);
        manageStreaming();
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
        if (olapChannel != null) {
            olapChannel.close();
        }
        if (this.messageBackupHandler != null) {
            this.messageBackupHandler.close();
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
                        addMessage(state, SENTINEL);
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
                addMessage(state, FAILURE);
            }
        }
    }

    public Throwable getFailure() {
        return failure;
    }
    
    private void manageStreaming() {
        if (!this.throttleEnabled)
            return;
        if (backupEnabled) {
            if (paused) {
                if (LOG.isTraceEnabled())
                    LOG.trace(String.format("StreamListener %s enables the backup mode and can continue to consume stream messages", uuid.toString()));
                continueStreaming();
            }
        } else {
            if (partitionStateMap.size() >= parallelPartitions * PARTITION_BUFFER_FACTOR && !paused) {
                if (LOG.isTraceEnabled())
                    LOG.trace(String.format("StreamListener %s has already %s partitions to process and could be overloaded, " +
                            "pause streaming", uuid.toString(), partitionStateMap.size()));
                pauseStreaming();
            } else if (partitionStateMap.size() < parallelPartitions * PARTITION_BUFFER_FACTOR && paused) {
                if (LOG.isTraceEnabled())
                    LOG.trace(String.format("StreamListener %s has already %s partitions to process and can continue to consume " +
                            "stream messages", uuid.toString(), partitionStateMap.size()));
                continueStreaming();
            }
        }
    }

    private void pauseStreaming() {
        if (olapChannel != null) {
            olapChannel.writeAndFlush(new StreamProtocol.PauseStream());
        }
        paused = true;
    }

    private void continueStreaming() {
        if (olapChannel != null) {
            olapChannel.writeAndFlush(new StreamProtocol.ContinueStream());
        }
        paused = false;
    }

    private void backupPartitions() {
        if (currentQueue > -1) {
            for (int partitionToBackup = currentQueue + PARTITION_BUFFER_FACTOR; partitionToBackup < numPartitions; partitionToBackup++) {
                if (partitionStateMap.containsKey(partitionToBackup)) {
                    messageBackupHandler.submitBackupTask(new BackupPartitionTask(this, partitionToBackup));
                }
            }
        }
    }

    void backupPartition(int partition) {
        if (!isBackupEnabled()) { //The client started to consume again and backup mode is not used any more
            return;
        }
        final PartitionState partitionState = partitionStateMap.get(partition);
        if (partitionState != null) {
            synchronized (partitionState) {
                if (partitionState.storage == ResultStorage.MEMORY && partitionState.messages.contains(SENTINEL)) {
                    if (LOG.isTraceEnabled())
                        LOG.trace(String.format("Backing up partition %s(%d)", uuid.toString(), partition));
                    partitionState.storage = ResultStorage.WRITING;
                    try {
                        messageBackupHandler.writeResults(partitionState.partition, partitionState.messages);
                        partitionState.messages = null;
                        partitionState.storage = ResultStorage.FILE;
                    } catch (Exception e) {
                        partitionState.storage = ResultStorage.MEMORY;
                        LOG.error(String.format("Could not backup up a partition %s(%d)", uuid.toString(), partition));
                    }
                    partitionState.notifyAll();
                }
            }
        }
    }

    void setBackupEnabled(boolean enabled) {
        if (enabled && !this.backupEnabled) {
            LOG.warn(String.format("Client is slow, backup mode is enabled for %s", this.uuid.toString()));
            this.backupEnabled = true;
            manageStreaming();
            try {
                this.backupPartitions();
            } catch (Exception e) {
                LOG.warn("Could not make a backup of partitions", e);
            }
        } else if (!enabled && this.backupEnabled) {
            LOG.info(String.format("Client is consuming again, backup mode is disabled for %s", this.uuid.toString()));
            this.backupEnabled = false;
            manageStreaming();
        }
    }

    boolean isBackupEnabled() {
        return backupEnabled;
    }

    Long getLastReadTime() {
        return lastReadTime;
    }

    private void writeToBackupIfEnabled(PartitionState state) {
        if (this.backupEnabled && state.partition >= currentQueue + PARTITION_BUFFER_FACTOR) {
            messageBackupHandler.submitBackupTask(new BackupPartitionTask(this, state.partition));
        }
    }

    private void loadMessages(PartitionState partitionState) throws IOException {
        if (partitionState.storage == ResultStorage.MEMORY)
            return;
        if (partitionState.storage != ResultStorage.FILE)
            throw new RuntimeException(String.format("Could not load a message backup %s (%d) %s", uuid.toString(),
                    partitionState.partition, partitionState.storage));
        partitionState.messages = messageBackupHandler.readResults(partitionState.partition);
        partitionState.storage = ResultStorage.MEMORY;
    }

    private void addMessage(PartitionState partitionState, Object message) {
        synchronized (partitionState) {
            try {
                if (partitionState.storage != ResultStorage.MEMORY) {
                    loadMessages(partitionState);
                }
                partitionState.messages.add(message);
            } catch (Exception e) {
                LOG.error(String.format("Could not restore messages for %s partition %d", uuid.toString(),
                        partitionState.partition));
                partitionState.messages = new ArrayBlockingQueue<>(queueSize + 4);
                partitionState.messages.add(FAILURE);
            }
        }
    }

    private ArrayBlockingQueue<Object> getMessages(PartitionState partitionState) {
        synchronized (partitionState) {
            try {
                if (partitionState.storage != ResultStorage.MEMORY) {
                    loadMessages(partitionState);
                }
            } catch (Exception e) {
                LOG.error(String.format("Could not restore messages for %s partition %d", uuid.toString(),
                        partitionState.partition));
                partitionState.messages = new ArrayBlockingQueue<>(queueSize + 4);
                partitionState.messages.add(FAILURE);
            }
            return partitionState.messages;
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
    volatile ResultStorage storage;

    PartitionState(int partition, int queueSize) {
        this.partition = partition;
        this.messages = new ArrayBlockingQueue<>(queueSize + 4);  // Extra to account for out of band messages
        this.storage = ResultStorage.MEMORY;
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
                ", storage=" + storage +
                '}';
    }
}
