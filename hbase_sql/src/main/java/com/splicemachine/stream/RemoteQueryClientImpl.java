package com.splicemachine.stream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.derby.stream.spark.BroadcastedActivation;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.timestamp.impl.TimestampPipelineFactoryLite;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.timestamp.impl.TimestampServerHandler;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.jboss.netty.bootstrap.ServerBootstrap;
import org.sparkproject.jboss.netty.buffer.ChannelBuffer;
import org.sparkproject.jboss.netty.buffer.ChannelBuffers;
import org.sparkproject.jboss.netty.channel.*;
import org.sparkproject.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.sparkproject.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.sparkproject.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.sparkproject.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.sparkproject.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.sparkproject.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;



/**
 * Created by dgomezferro on 5/20/16.
 */
public class RemoteQueryClientImpl extends SimpleChannelHandler implements RemoteQueryClient, ChannelPipelineFactory, Iterator<LocatedRow> {
    private static final Logger LOG = Logger.getLogger(RemoteQueryClientImpl.class);

    private final SpliceBaseOperation root;
    private NioServerSocketChannelFactory factory;
    private Channel serverChannel;
    private Map<Channel, Integer> partitionMap = new ConcurrentHashMap<>();
    private ArrayBlockingQueue[] messages;
    private boolean initialized = false;

    private LocatedRow currentResult;
    private int currentQueue = 0;

    public RemoteQueryClientImpl(SpliceBaseOperation root) {
        this.root = root;
    }

    @Override
    public void submit() throws StandardException {
        ActivationHolder ah = new ActivationHolder(root.getActivation());

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("TimestampServer-%d").setDaemon(true).build());
        factory = new NioServerSocketChannelFactory(executor, executor);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);

        bootstrap.setPipelineFactory(this);

        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);

        this.serverChannel = bootstrap.bind(new InetSocketAddress(0));
        InetSocketAddress socketAddress = (InetSocketAddress)this.serverChannel.getLocalAddress();
        String host = socketAddress.getHostName();
        int port = socketAddress.getPort();

        RemoteQueryJob jobRequest = new RemoteQueryJob(ah, root.getResultSetNumber(), host, port);
        try {
            final Future<OlapResult> future = EngineDriver.driver().getOlapClient().submit(jobRequest);
            new Thread() {
                @Override
                public void run() {
                    SConfiguration conf = HConfiguration.getConfiguration();
                    while(true) {
                        try {
                            LOG.warn("Checking job status");
                            OlapResult result = future.get(conf.getOlapClientTickTime(), TimeUnit.MILLISECONDS);

                            LOG.warn("Finished!");
                            return;
                        } catch (InterruptedException e) {

                            LOG.warn("Error!" ,e);
                            e.printStackTrace();
                            return;
                        } catch (ExecutionException e) {
                            LOG.warn("Error!" ,e);
                            e.printStackTrace();
                            return;
                        } catch (TimeoutException e) {
                            LOG.warn("Timed out, ignore");
                            //ignore
                        }
                    }
                }
            }.start();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public Iterator<LocatedRow> getIterator() {
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
        advance();
        return this;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        SpliceLogUtils.debug(LOG, "Creating new channel pipeline...");
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("encoder", new ObjectEncoder());
        pipeline.addLast("decoder", new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
        pipeline.addLast("handler", this);
        SpliceLogUtils.debug(LOG, "Done creating channel pipeline");
        return pipeline;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error("Exception caught");
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        Channel channel = ctx.getChannel();

        LOG.warn("Received " + msg + " from " + channel);

        if (msg instanceof Long) {
            synchronized (this) {
                if (initialized)
                    return;
                Long partitions = (Long) msg;
                messages = new ArrayBlockingQueue[(int) (long) partitions];
                for (int i = 0; i < partitions; i++) {
                    messages[i] = new ArrayBlockingQueue(1024);
                }
                initialized = true;
            }
        } else if (msg instanceof Integer) {
            Integer partition = (Integer) msg;
            partitionMap.put(channel, partition);
        } else {
            Integer partition = partitionMap.get(channel);
            messages[partition].put(msg);
        }
    }

    @Override
    public boolean hasNext() {
        return currentResult != null;
    }

    @Override
    public LocatedRow next() {
        LocatedRow result = currentResult;
        advance();
        return result;
    }

    private void advance() {
        LocatedRow next = null;
        try {
            while (next == null) {

                LOG.warn("Advancing to next message");
                Object msg = messages[currentQueue].take();
                LOG.warn("Next message: " + msg);
                if (msg instanceof String) {
                    LOG.warn("Moving queues");
                    currentQueue++;
                    if (currentQueue >= messages.length) {
                        LOG.warn("The end");
                        // finished
                        currentResult = null;
                        return;
                    }
                } else {
                    next = (LocatedRow) msg;
                }
            }
            currentResult = next;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
