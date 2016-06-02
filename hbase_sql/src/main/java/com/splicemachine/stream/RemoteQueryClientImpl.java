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
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.SpliceSparkKryoRegistrator;
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

    private final SpliceBaseOperation root;
    private StreamListener streamListener;

    public RemoteQueryClientImpl(SpliceBaseOperation root) {
        this.root = root;
    }

    @Override
    public void submit() throws StandardException {
        ActivationHolder ah = new ActivationHolder(root.getActivation());

        try {
            streamListener = new StreamListener();
            HostAndPort hostAndPort = streamListener.start();
            String host = hostAndPort.getHostText();
            int port = hostAndPort.getPort();

            RemoteQueryJob jobRequest = new RemoteQueryJob(ah, root.getResultSetNumber(), host, port);
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
        return streamListener.getIterator();
    }
}
