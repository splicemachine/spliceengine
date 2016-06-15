package com.splicemachine.stream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.EngineDriver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.RemoteQueryClient;
import com.splicemachine.derby.stream.spark.BroadcastedActivation;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.timestamp.impl.TimestampPipelineFactoryLite;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.timestamp.impl.TimestampServerHandler;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoPool;
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
import java.util.*;
import java.util.concurrent.*;



/**
 * Created by dgomezferro on 5/20/16.
 */
@ChannelHandler.Sharable
public class RemoteQueryClientImpl implements RemoteQueryClient {
    private static final Logger LOG = Logger.getLogger(RemoteQueryClientImpl.class);

    private static StreamListenerServer server;

    private final SpliceBaseOperation root;
    private StreamListener streamListener;
    private long offset = 0;
    private long limit = -1;

    public RemoteQueryClientImpl(SpliceBaseOperation root) {
        this.root = root;
    }

    private StreamListenerServer getServer() throws StandardException {
        synchronized (RemoteQueryClientImpl.class) {
            if (server == null) {
                server = new StreamListenerServer(59599);
                server.start();
            }
        }
        return server;
    }

    @Override
    public void submit() throws StandardException {
        ActivationHolder ah = new ActivationHolder(root.getActivation());

        try {
            updateLimitOffset();
            int streamingBatches = HConfiguration.getConfiguration().getSparkResultStreamingBatches();
            int streamingBatchSize = HConfiguration.getConfiguration().getSparkResultStreamingBatchSize();
            streamListener = new StreamListener(limit, offset, streamingBatches, streamingBatchSize);
            StreamListenerServer server = getServer();
            server.register(streamListener);
            HostAndPort hostAndPort = server.getHostAndPort();
            String host = hostAndPort.getHostText();
            int port = hostAndPort.getPort();
            UUID uuid = streamListener.getUuid();

            RemoteQueryJob jobRequest = new RemoteQueryJob(ah, root.getResultSetNumber(), uuid, host, port);
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

    private void updateLimitOffset() throws StandardException {
        if (root instanceof ScrollInsensitiveOperation
                || root instanceof AnyOperation
                || root instanceof OnceOperation) {

            SpliceOperation source = root.getSubOperations().get(0);
            if (!(source instanceof RowCountOperation))
                return;

            RowCountOperation rco = (RowCountOperation) source;
            this.limit = rco.getFetchLimit();
            this.offset = rco.getTotalOffset();
            if (this.offset == -1) {
                offset = 0;
            }
        }
    }

    @Override
    public Iterator<LocatedRow> getIterator() {
        return streamListener.getIterator();
    }
}
