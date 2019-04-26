/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.CountDownLatches;
import com.splicemachine.timestamp.api.Callback;
import com.splicemachine.timestamp.api.TimestampClientStatistics;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

/**
 * Accepts concurrent requests for new transactional timestamps and
 * sends them over a shared connection to the remote {@link TimestampServer}.
 * For the caller, the invocation of {@link #getNextTimestamp()}
 * is synchronous.
 * <p>
 * This class should generally not be constructed directly.
 *
 * @author Walt Koetke
 */
public class TimestampClient extends TimestampBaseHandler implements TimestampClientStatistics{


    private static final Logger LOG = Logger.getLogger(TimestampClient.class);

    private static final short CLIENT_COUNTER_INIT = 100; // actual value doesn't matter

    /**
     * Fixed number of bytes in the message we expect to receive back from the server.
     */
    private static final int FIXED_MSG_RECEIVED_LENGTH = 10; // 2 byte client id + 8 byte timestamp

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, SHUTDOWN
    }

    /**
     * A map representing all currently active callers to this TimestampClient
     * waiting for their response.
     */
    private ConcurrentMap<Short, Callback> clientCallbacks = null;

    private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);

    private ClientBootstrap bootstrap;
    private volatile Channel channel;
    private NioClientSocketChannelFactory factory;

    /**
     * Internal unique identifier for a single synchronous call to this instance
     * of {@link TimestampClient}. Necessary in order to subsequently associate
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


    public TimestampClient(int timeoutMillis,TimestampHostProvider timestampHostProvider) {
        this.timeoutMillis = timeoutMillis;
        this.timestampHostProvider = timestampHostProvider;
        clientCallbacks = new ConcurrentHashMap<>();
        
        ExecutorService workerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-worker-%d").setDaemon(true).build());
        ExecutorService bossExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-boss-%d").setDaemon(true).build());

        factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);

        bootstrap = new ClientBootstrap(factory);

        // If we end up needing to use one of the memory aware executors,
        // do so with code like this (leave commented out for reference).
        //
        // bootstrap.getPipeline().addLast("executor", new ExecutionHandler(
        // 	   new OrderedMemoryAwareThreadPoolExecutor(10 /* threads */, 1024*1024, 4*1024*1024)));

        bootstrap.getPipeline().addLast("decoder", new FixedLengthFrameDecoder(FIXED_MSG_RECEIVED_LENGTH));
        bootstrap.getPipeline().addLast("handler", this);

        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        // bootstrap.setOption("connectTimeoutMillis", 120000);

        // Would be nice to try connecting here, but not sure if this works right. connectIfNeeded();

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
            if(state==State.SHUTDOWN) return;
            shouldContinue=!this.state.compareAndSet(state,State.SHUTDOWN);
        }
        LOG.info(String.format("shutting down TimestampClient state=%s", this.state.get()));
        try {
            this.state.set(State.SHUTDOWN);
            if (channel != null && channel.isOpen()) {
                channel.close().awaitUninterruptibly();
            }
            factory.releaseExternalResources();
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
                    continue;
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
            futureConnect.addListener(new ChannelFutureListener() {
                                          public void operationComplete(ChannelFuture cf) throws Exception {
                                              if (cf.isSuccess()) {
                                                  channel = cf.getChannel();
                                                  latchConnect.countDown();
                                              } else {
                                                  latchConnect.countDown();
                                                  doClientErrorThrow(LOG, "TimestampClient unable to connect to TimestampServer", cf.getCause());
                                              }
                                          }
                                      }
            );

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

    public long refresh() throws TimestampIOException {
        return getNextTimestamp(true);
    }

    public long getNextTimestamp() throws TimestampIOException {
        return getNextTimestamp(false);
    }

    public long getNextTimestamp(boolean refresh) throws TimestampIOException {

        // Measure duration of full client request for JMX
        long requestStartTime = System.currentTimeMillis();

        connectIfNeeded();

        short clientCallId = (short) clientCallCounter.getAndIncrement();
        final ClientCallback callback = new ClientCallback(clientCallId);
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
            ChannelBuffer buffer = ChannelBuffers.buffer(3);
            buffer.writeShort(clientCallId);
            buffer.writeByte(refresh?1:0);
            SpliceLogUtils.trace(LOG, "Writing request message to server for client: %s", callback);
            if(channel == null) {
                throw new TimestampIOException("Unable to connect to TimestampServer");
            }
            ChannelFuture futureWrite = channel.write(buffer);
            futureWrite.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        clientCallbacks.remove(clientCallId);
                        doClientErrorThrow(LOG, "Error writing message from timestamp client to server", future.getCause());
                    } else {
                        SpliceLogUtils.trace(LOG, "Request sent. Waiting for response for client: %s", callback);
                    }
                }
            });
        } catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
            clientCallbacks.remove(clientCallId);
            callback.error(e);
            doClientErrorThrow(LOG, "Exception writing message to timestamp server for client: %s", e, callback);
        }

        // If we get here, request was successfully sent without exception.
        // However, we might not have received response yet, so we need to
        // wait for that now.

        try {
            boolean success = callback.await(timeoutMillis);
            if (!success) {
                // We timed out, close the channel so that the next request recreates the connection
                channel.close();
                clientCallbacks.remove(clientCallId);
                
                doClientErrorThrow(LOG, "Client timed out after %s ms waiting for new timestamp: %s", null, timeoutMillis, callback);
            }
        } catch (InterruptedException e) {
            clientCallbacks.remove(clientCallId);
            doClientErrorThrow(LOG, "Interrupted waiting for timestamp client: %s", e, callback);
        }

        // If we get here, it should mean the client received the response with the timestamp,
        // which we can fetch now from the callback and send it back to the caller.

        long timestamp = callback.getNewTimestamp();
        if (timestamp < 0) {
            clientCallbacks.remove(clientCallId); // defensive call, this should have already been removed
            doClientErrorThrow(LOG, "Invalid timestamp found for client: %s", null, callback);
        }

        SpliceLogUtils.debug(LOG, "Client call complete: %s", callback);

        // Since request was successful, update JMX metrics
        numRequests.incrementAndGet();
        totalRequestDuration.addAndGet(System.currentTimeMillis() - requestStartTime);

        return timestamp;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        assert (buf != null);
        ensureReadableBytes(buf, FIXED_MSG_RECEIVED_LENGTH);

        short clientCallerId = buf.readShort();
        ensureReadableBytes(buf, 8);

        long timestamp = buf.readLong();
        assert (timestamp > 0);
        ensureReadableBytes(buf, 0);

        SpliceLogUtils.debug(LOG, "Response from server: clientCallerId = %s, timestamp = %s", clientCallerId, timestamp);
        Callback cb = clientCallbacks.remove(clientCallerId);
        if (cb == null) {
            doClientErrorThrow(LOG, "Client callback with id %s not found, so unable to deliver timestamp %s", null, clientCallerId, timestamp);
        }

        // This releases the latch the original client thread is waiting for
        // (to provide the synchronous behavior for that caller) and also
        // provides the timestamp.
        cb.complete(timestamp);

        super.messageReceived(ctx, e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        SpliceLogUtils.info(LOG, "Successfully connected to server");
        channel = e.getChannel();
        state.set(State.CONNECTED);
        super.channelConnected(ctx, e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        LOG.info("TimestampClient was disconnected from the server");
        boolean shouldContinue;
        do{
            State s = state.get();
            if(s==State.SHUTDOWN) return; //ignore shut down errors
            channel=null;
            shouldContinue = !state.compareAndSet(s,State.DISCONNECTED);
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if(state.get() != State.SHUTDOWN) {
            LOG.error("exceptionCaught", e.getCause());
        }
    }

    public static void doClientErrorThrow(Logger logger, String message, Throwable t, Object... args) throws TimestampIOException {
        if (message == null) message = "";
        message = String.format(message, args);
        TimestampIOException t1 = t != null ? new TimestampIOException(message, t) : new TimestampIOException(message);
        SpliceLogUtils.logAndThrow(logger, message, t1);
    }
}
