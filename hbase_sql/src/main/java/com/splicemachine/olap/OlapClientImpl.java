package com.splicemachine.olap;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.CountDownLatches;
import com.splicemachine.derby.iapi.sql.olap.OlapCallable;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class OlapClientImpl extends SimpleChannelHandler implements OlapClient {


    private static final Logger LOG = Logger.getLogger(OlapClientImpl.class);

    private static final short CLIENT_COUNTER_INIT = 100; // actual value doesn't matter
    private final String host;
    private final int port;
    private final Clock clock;


    public OlapClientImpl(String host, int port, int timeoutMillis, Clock clock) {
        this.host = host;
        this.port = port;
        this.timeoutMillis = timeoutMillis;
        this.clock = clock;

        clientCallbacks = new ConcurrentHashMap<>();

        ExecutorService workerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("olapClient-worker-%d").setDaemon(true).build());
        ExecutorService bossExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("olapClient-boss-%d").setDaemon(true).build());

        factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);

        bootstrap = new ClientBootstrap(factory);

        bootstrap.getPipeline().addLast("decoder", new ObjectDecoder(ClassResolvers.weakCachingResolver(null)));
        bootstrap.getPipeline().addLast("encoder", new ObjectEncoder());
        bootstrap.getPipeline().addLast("handler", this);

        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);

        // Would be nice to try connecting here, but not sure if this works right. connectIfNeeded();
    }

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, SHUTDOWN
    }

    /**
     * A map representing all currently active callers to this OlapClient
     * waiting for their response.
     */
    private ConcurrentMap<Short, Callback> clientCallbacks = null;

    private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);

    private ClientBootstrap bootstrap;
    private volatile Channel channel;
    private NioClientSocketChannelFactory factory;

    /**
     * Internal unique identifier for a single synchronous call to this instance
     * of {@link OlapClientImpl}. Necessary in order to subsequently associate
     * a server response with the original request. Although this is an atomic integer,
     * we consume it internally as a short so that we only pass two bytes (not four)
     * over the wire.
     */
    // We might even get away with using a byte here (256 concurrent client calls),
    // but use a short just in case.
    private AtomicInteger clientCallCounter = new AtomicInteger(CLIENT_COUNTER_INIT);

    int timeoutMillis;

    public void shutdown() {
        LOG.info(String.format("shutting down OlapClient state=%s", this.state.get()));
        boolean shouldContinue = true;
        while(shouldContinue){
            State state=this.state.get();
            if(state== State.SHUTDOWN) return;
            shouldContinue=!this.state.compareAndSet(state, State.SHUTDOWN);
        }
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
        return port;
    }



    /*
    Returns when connected or raises an exception
     */
    protected void connectIfNeeded() throws IOException {

        // Even though state is an atomic reference, synchronize on whole block
        // including code that attempts connection. Otherwise, two threads might
        // end up trying to connect at the same time.

        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "Connect if needed state %s", state.get());
        }

        boolean shouldContinue = true;
        while(shouldContinue){
            State s = state.get();
            switch (s){
                case DISCONNECTED:
                    shouldContinue = !state.compareAndSet(s, State.CONNECTING);
                    if (LOG.isTraceEnabled()) {
                        SpliceLogUtils.trace(LOG, "Connect if needed (should continue: %s, state %s)", shouldContinue, state.get());
                    }
                    break;
                case CONNECTING:
                    waitFor(State.CONNECTED);
                    return;
                case CONNECTED:
                    return;
                case SHUTDOWN:
                    throw new IOException("Client is shuting down");
            }
        }

        if (LOG.isInfoEnabled()) {
            SpliceLogUtils.info(LOG, "Attempting to connect to server (host %s, port %s)", host, getPort());
        }

        ChannelFuture futureConnect = bootstrap.connect(new InetSocketAddress(host, getPort()));
        final CountDownLatch latchConnect = new CountDownLatch(1);
        futureConnect.addListener(new ChannelFutureListener() {
                                      public void operationComplete(ChannelFuture cf) throws Exception {
                                          if (cf.isSuccess()) {

                                              if (LOG.isInfoEnabled()) {
                                                  SpliceLogUtils.info(LOG, "Connection successful");
                                              }
                                              channel = cf.getChannel();
                                              latchConnect.countDown();
                                          } else {
                                              latchConnect.countDown();
                                              doClientErrorThrow(LOG, "OlapClient unable to connect to OlapServer", cf.getCause());
                                          }
                                      }
                                  }
        );

        CountDownLatches.uncheckedAwait(latchConnect);
        if(channel == null) {
            throw new IOException("Unable to connect to OlapServer");
        }

        if (LOG.isInfoEnabled()) {
            SpliceLogUtils.info(LOG, "Successfully connected to server (host %s, port %s)", host, getPort());
        }

        state.set(State.CONNECTED);
    }

    private void waitFor(State endState) throws IOException {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "Waiting for %s, current state %s", endState, state.get());
        }
        long startTime = clock.currentTimeMillis();
        while(clock.currentTimeMillis() - startTime < timeoutMillis) {
            try {
                clock.sleep(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            if (state.get().equals(endState)) {
                return;
            }
        }
        throw new IOException("waitFor " +endState+" timeout, waited " + (clock.currentTimeMillis() - startTime));
    }

    @Override
    public <R extends OlapResult> R submitOlapJob(OlapCallable<R> callable) throws IOException {

        connectIfNeeded();

        short clientCallId = (short) clientCallCounter.getAndIncrement();
        final ClientCallback callback = new ClientCallback(clientCallId);
        SpliceLogUtils.debug(LOG, "Starting new client call with id %s", clientCallId);

        // Add this caller (id and callback) to the map of current clients.
        // If an entry was already present for this caller id, that is a bug,
        // so throw an exception.
        if (clientCallbacks.putIfAbsent(clientCallId, callback) != null) {
            doClientErrorThrow(LOG, "Found existing client callback with caller id %s, so unable to handle new call.", null, clientCallId);
        }

        try {
            SpliceLogUtils.trace(LOG, "Writing request message to server for client: %d", clientCallId);
            callable.setCallerId(clientCallId);
            ChannelFuture futureWrite = channel.write(callable);
            futureWrite.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        doClientErrorThrow(LOG, "Error writing message from olap client to server", future.getCause());
                    } else {
                        SpliceLogUtils.trace(LOG, "Request sent. Waiting for response for client: %s", callback);
                    }
                }
            });
        } catch (Exception e) { // Correct to catch all Exceptions in this case so we can remove client call
            clientCallbacks.remove(clientCallId);
            callback.error(e);
            doClientErrorThrow(LOG, "Exception writing message to olap server for client: %d", e, clientCallId);
        }

        // If we get here, request was successfully sent without exception.
        // However, we might not have received response yet, so we need to
        // wait for that now.

        try {
            boolean success = callback.await(timeoutMillis);
            if (!success) {
                doClientErrorThrow(LOG, "Client timed out after %s ms waiting for olap server: %s", null, timeoutMillis, callback);
            }
        } catch (InterruptedException e) {
            doClientErrorThrow(LOG, "Interrupted waiting for olap client: %s", e, callback);
        }

        // If we get here, it should mean the client received the response with the timestamp,
        // which we can fetch now from the callback and send it back to the caller.

        if (callback.getThrowable() != null) {
            throw new IOException(callback.getThrowable());
        }
        return (R) callback.getResult();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        OlapResult result = (OlapResult) e.getMessage();

        short clientCallerId = result.getCallerId();
        SpliceLogUtils.debug(LOG, "Response from server: clientCallerId = %s", clientCallerId);
        Callback cb = clientCallbacks.remove(clientCallerId);
        if (cb == null) {
            doClientErrorThrow(LOG, "Client callback with id %s not found", null, clientCallerId);
        }

        // This releases the latch the original client thread is waiting for
        // (to provide the synchronous behavior for that caller) and also
        // provides the timestamp.
        if (result.getThrowable() != null) {
            SpliceLogUtils.error(LOG, "Olap result for %d has an error %s", clientCallerId, result.getThrowable());
            cb.error(result.getThrowable());
        } else {
            cb.complete(result);
        }

        super.messageReceived(ctx, e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        LOG.info("OlapClient was disconnected from the server");
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
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if(state.get() != State.SHUTDOWN) {
            LOG.error("exceptionCaught", e.getCause());
        }
    }

    public static void doClientErrorThrow(Logger logger, String message, Throwable t, Object... args) throws IOException {
        if (message == null) message = "";
        IOException t1 = t != null ? new IOException(message, t) : new IOException(message);
        SpliceLogUtils.logAndThrow(logger, String.format(message, args), t1);
    }
}
