package com.splicemachine.stream;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.spark.api.java.function.Function2;
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
import org.sparkproject.jboss.netty.bootstrap.ClientBootstrap;
import org.sparkproject.jboss.netty.channel.Channel;
import org.sparkproject.jboss.netty.channel.ChannelFuture;
import org.sparkproject.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.sparkproject.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.sparkproject.jboss.netty.handler.codec.serialization.ObjectEncoder;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dgomezferro on 5/25/16.
 */
public class ResultStreamer implements Function2<Integer, Iterator<LocatedRow>, Iterator<Object>>, Serializable {
    private int numPartitions;
    private String host;
    private int port;

    // Serialization
    public ResultStreamer() {
    }

    public ResultStreamer(String host, int port, int numPartitions) {
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
    }

    @Override
    public Iterator<Object> call(Integer partition, Iterator<LocatedRow> locatedRowIterator) throws Exception {

        InetSocketAddress socketAddr=new InetSocketAddress(host,port);

        ClientBootstrap bootstrap;
        ExecutorService workerExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-worker-%d").setDaemon(true).build());
        ExecutorService bossExecutor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("timestampClient-boss-%d").setDaemon(true).build());

        NioClientSocketChannelFactory factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);

        bootstrap = new ClientBootstrap(factory);

        bootstrap.getPipeline().addLast("encoder", new ObjectEncoder());

        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);

        ChannelFuture futureConnect = bootstrap.connect(socketAddr);

        futureConnect.await();
        Channel channel = futureConnect.getChannel();
        channel.write((long)numPartitions);
        channel.write(partition);

        while (locatedRowIterator.hasNext()) {
            LocatedRow lr = locatedRowIterator.next();
            channel.write(lr);
        }
        channel.write(new String("FIN")).await();
        channel.close().await();
        bootstrap.releaseExternalResources();
        return Collections.emptyIterator();
    }
}
