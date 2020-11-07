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

package com.splicemachine.timestamp.impl;

import com.splicemachine.concurrent.CountDownLatches;
import com.splicemachine.timestamp.api.Callback;
import com.splicemachine.timestamp.api.TimestampClientStatistics;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.utils.SpliceLogUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import org.apache.log4j.Logger;
import splice.com.google.common.util.concurrent.ThreadFactoryBuilder;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages a connection to the Timestamp Server
 */
@ChannelHandler.Sharable
public class TimestampConnection extends TimestampBaseHandler<TimestampMessage.TimestampResponse> implements TimestampClientStatistics {


    private static final Logger LOG = Logger.getLogger(TimestampConnection.class);

    private static final short CLIENT_COUNTER_INIT = 100; // actual value doesn't matter

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, SHUTDOWN
    }

    /**
     * A map representing all currently active callers to this TimestampClient
     * waiting for their response.
     */
    private ConcurrentMap<Short, Callback> clientCallbacks = null;

    private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);

    private volatile Channel channel;

    /**
     * Internal unique identifier for a single synchronous call to this instance
     * of {@link TimestampConnection}. Necessary in order to subsequently associate
     * a server response with the original request. Although this is an atomic integer,
     * we consume it internally as a short so that we only pass two bytes (not four)
     * over the wire.
     */
    // We might even get away with using a byte here (256 concurrent client calls),
    // but use a short just in case.
    private AtomicInteger clientCallCounter = new AtomicInteger(CLIENT_COUNTER_INIT);

    int timeoutMillis;

    // Metrics to expose via JMX. See TimestampClientStatistics
    // for solid definitions of each metric.
    private AtomicLong numRequests = new AtomicLong(0);
    private AtomicLong totalRequestDuration = new AtomicLong(0);
    private TimestampHostProvider timestampHostProvider;
    private Bootstrap bootstrap;
    private EventLoopGroup workerGroup;


    public TimestampConnection(int timeoutMillis, TimestampHostProvider timestampHostProvider) {
        this.timeoutMillis = timeoutMillis;
        this.timestampHostProvider = timestampHostProvider;
        clientCallbacks = new ConcurrentHashMap<>();

        workerGroup =  new NioEventLoopGroup(2, new ThreadFactoryBuilder().setNameFormat("TimestampClient-worker-%d").setDaemon(true).build());

        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                .addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                .addLast("protobufDecoder", new ProtobufDecoder(TimestampMessage.TimestampResponse.getDefaultInstance()))
                .addLast("frameEncoder", new LengthFieldPrepender(4))
                .addLast("protobufEncoder", new ProtobufEncoder())
                .addLast("handler", TimestampConnection.this);
            }
        });

        try {
            registerJMX();
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unable to register TimestampClient with JMX. Timestamps will still be generated but metrics will not be available.");
        }
    }

    public void shutdown() {
        boolean shouldContinue = true;
        while(shouldContinue){
            State state=this.state.get();
            if(state== State.SHUTDOWN) return;
            shouldContinue=!this.state.compareAndSet(state, State.SHUTDOWN);
        }
        LOG.info(String.format("shutting down TimestampClient state=%s", this.state.get()));
        try {
            this.state.set(State.SHUTDOWN);
            workerGroup.shutdownGracefully();
        } catch (Throwable t) {
            LOG.error("error shutting down", t);
        }
    }

    /**
     * Returns the port number which the client should use when connecting
     * to the timestamp server.
     */
    protected int getPort() {
        return timestampHostProvider.getPort();
    }

    protected void connectIfNeeded() throws TimestampIOException{

        // Even though state is an atomic reference, synchronize on whole block
        // including code that attempts connection. Otherwise, two threads might
        // end up trying to connect at the same time.

        boolean shouldContinue = true;
        while(shouldContinue){
            State s = state.get();
            switch (s) {
                case CONNECTED:
                    return;
                case DISCONNECTED:
                    shouldContinue = !state.compareAndSet(s, State.CONNECTING);
                    break;
                case SHUTDOWN:
                    throw new TimestampIOException("Shutting down");
                default:
                    // somebody else is trying to connect
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new TimestampIOException("Interrupted", e);
                    }
            }
        }

        try {

            if (LOG.isInfoEnabled()) {
                SpliceLogUtils.info(LOG, "Attempting to connect to server (host %s, port %s)", timestampHostProvider.getHost(), getPort());
            }

            // clear clientCallback mappings
            clientCallbacks.clear();

            ChannelFuture futureConnect = bootstrap.connect(new InetSocketAddress(timestampHostProvider.getHost(), getPort()));
            final CountDownLatch latchConnect = new CountDownLatch(1);
            futureConnect.addListener(cf -> {
                if (cf.isSuccess()) {
                    channel = ((ChannelPromise)cf).channel();
                    latchConnect.countDown();
                } else {
                    latchConnect.countDown();
                    throwClientError(LOG, "TimestampClient unable to connect to TimestampServer", cf.cause());
                }
            });

            CountDownLatches.uncheckedAwait(latchConnect);
            if (channel == null) {
                throw new TimestampIOException("Unable to connect to TimestampServer");
            }
        } finally {
            if (channel == null) {
                LOG.error("We couldn't connect to the TimestampServer, reset the state to DISCONNECTED");
                // Set state to disconnected so that the next call to connectIfNeeded() has a chance to try to reconnect
                state.set(State.DISCONNECTED);
            }
        }

    }

    public long[] getBatchTimestamps(int batchSize) throws TimestampIOException {
        TimestampMessage.TimestampRequest.Builder requestBuilder = TimestampMessage.TimestampRequest.newBuilder()
                .setTimestampRequestType(TimestampMessage.TimestampRequestType.GET_TIMESTAMP_BATCH)
                .setTimestampBatch(TimestampMessage.GetTimestampBatch.newBuilder().setBatchSize(batchSize));
        TimestampMessage.GetTimestampBatchResponse response = issueRequest(requestBuilder).getTimestampBatchResponse();
        int size = response.getBatchSize();
        long firstTS = response.getFirstTimestamp();
        int increment = response.getTimestampDelta();
        long[] result = new long[size];
        for (int i = 0; i < size; ++i) {
            result[i] = firstTS + increment*i;
        }
        return result;
    }

    public long getSingleTimestamp() throws TimestampIOException {
        TimestampMessage.TimestampRequest.Builder requestBuilder = TimestampMessage.TimestampRequest.newBuilder()
                .setTimestampRequestType(TimestampMessage.TimestampRequestType.GET_NEXT_TIMESTAMP);
        return issueRequest(requestBuilder).getGetNextTimestampResponse().getTimestamp();
    }

    TimestampMessage.TimestampResponse issueRequest(TimestampMessage.TimestampRequest.Builder requestBuilder) throws TimestampIOException {

        // Measure duration of full client request for JMX
        long requestStartTime = System.currentTimeMillis();

        connectIfNeeded();

        short clientCallId = (short) clientCallCounter.getAndIncrement();
        requestBuilder.setCallerId(clientCallId);
        final ClientCallback callback = new ClientCallback();
        SpliceLogUtils.debug(LOG, "Starting new client call with id %s", clientCallId);

        // Add this caller (id and callback) to the map of current clients.
        // If an entry was already present for this caller id, that is a bug,
        // so throw an exception.
        if (clientCallbacks.putIfAbsent(clientCallId, callback) != null) {
            String msg = String.format("Found existing client callback with caller id %s, so unable to handle new call.", clientCallId);
            LOG.error(msg + " Callback map size = " + clientCallbacks.size());
            throw new TimestampIOException(msg);
        }

        try {
            SpliceLogUtils.trace(LOG, "Writing request message to server for client: %s", callback);
            if(channel == null) {
                throw new TimestampIOException("Unable to connect to TimestampServer");
            }
            ChannelFuture futureWrite = channel.writeAndFlush(requestBuilder.build());
            futureWrite.addListener(future -> {
                if (!future.isSuccess()) {
                    throwClientErrorAndClean(clientCallId, "Error writing message from timestamp client to server", future.cause());
                } else {
                    SpliceLogUtils.trace(LOG, "Request sent. Waiting for response for client: %s", callback);
                }
            });
        } catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
            callback.error(e);
            throwClientErrorAndClean(clientCallId, "Exception writing message to timestamp server for client: %s", e, callback);
        }

        // If we get here, request was successfully sent without exception.
        // However, we might not have received response yet, so we need to
        // wait for that now.

        try {
            boolean success = callback.await(timeoutMillis);
            if (!success) {
                // We timed out, close the channel so that the next request recreates the connection
                channel.close();
                throwClientErrorAndClean(clientCallId, "Client timed out after %s ms waiting for new timestamp: %s", new TimeoutException(), timeoutMillis, callback);
            }
        } catch (InterruptedException e) {
            throwClientErrorAndClean(clientCallId, "Interrupted waiting for timestamp client: %s", e, callback);
        }

        // If we get here, it should mean the client received the response with the timestamp,
        // which we can fetch now from the callback and send it back to the caller.

        if (callback.getException() != null) {
            throwClientErrorAndClean(clientCallId, "Received remote exception", callback.getException());
        }
        if (callback.responseIsInvalid()) {
            throwClientErrorAndClean(clientCallId, "Invalid timestamp found for client: %s", null, callback);
        }

        SpliceLogUtils.debug(LOG, "Client call complete: %s", callback);

        // Since request was successful, update JMX metrics
        numRequests.incrementAndGet();
        totalRequestDuration.addAndGet(System.currentTimeMillis() - requestStartTime);

        return callback.getResponse();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TimestampMessage.TimestampResponse response) throws Exception {
        short clientCallerId = (short) response.getCallerId();

        SpliceLogUtils.debug(LOG, "Response from server: clientCallerId = %s, response = %s", clientCallerId, response);
        Callback cb = clientCallbacks.remove(clientCallerId);
        if (cb == null) {
            throwClientError(LOG, "Client callback with id %s not found, so unable to deliver response %s", null, clientCallerId, response);
        }

        // This releases the latch the original client thread is waiting for
        // (to provide the synchronous behavior for that caller) and also
        // provides the timestamp.
        assert cb != null;
        if (response.hasErrorMessage()) {
            cb.error(new TimestampIOException(response.getErrorMessage()));
        } else {
            cb.complete(response);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        SpliceLogUtils.info(LOG, "Successfully connected to server");
        channel = ctx.channel();
        state.set(State.CONNECTED);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("TimestampClient was disconnected from the server");
        boolean shouldContinue;
        do{
            State s = state.get();
            if(s== State.SHUTDOWN) return; //ignore shut down errors
            channel=null;
            shouldContinue = !state.compareAndSet(s, State.DISCONNECTED);
        }while(shouldContinue);
        connectIfNeeded();
    }

    @Override
    protected void doError(String message, Throwable t, Object... args) {
        SpliceLogUtils.error(LOG, message, t, args);
    }

    private void registerJMX() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        registerJMX(mbs);
        SpliceLogUtils.info(LOG, "TimestampClient on region server successfully registered with JMX");
    }

    private void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName name = new ObjectName("com.splicemachine.si.client.timestamp.request:type=TimestampClientStatistics"); // Same string is in JMXUtils
        mbs.registerMBean(this, name);
    }

    @Override
    public long getNumberTimestampRequests() {
        return numRequests.get();
    }

    @Override
    public double getAvgTimestampRequestDuration() {
        double a = (double) totalRequestDuration.get();
        double b = (double) numRequests.get();
        return a / b;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if(state.get() != State.SHUTDOWN) {
            LOG.error("exceptionCaught", cause);
        }
    }

    public static void throwClientError(Logger LOG, String message, Throwable t, Object... args) throws TimestampIOException {
        if (message == null) message = "";
        message = String.format(message, args);
        TimestampIOException t1 = t != null ? new TimestampIOException(message, t) : new TimestampIOException(message);
        SpliceLogUtils.logAndThrow(LOG, message, t1);
    }

    private void throwClientErrorAndClean(short callerId, String message, Throwable t, Object... args) throws TimestampIOException {
        clientCallbacks.remove(callerId);
        throwClientError(LOG, message, t, args);
    }
}
