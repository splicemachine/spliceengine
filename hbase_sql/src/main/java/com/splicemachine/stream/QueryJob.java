package com.splicemachine.stream;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.compactions.CompactionInputFormat;
import com.splicemachine.compactions.CompactionResult;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.ExplainOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.spark.SparkFlatMapFunction;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.olap.DistributedCompaction;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.sparkproject.guava.util.concurrent.ThreadFactoryBuilder;
import org.sparkproject.jboss.netty.bootstrap.ClientBootstrap;
import org.sparkproject.jboss.netty.channel.Channel;
import org.sparkproject.jboss.netty.channel.ChannelFuture;
import org.sparkproject.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.sparkproject.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.sparkproject.jboss.netty.handler.codec.serialization.ObjectEncoder;
import scala.Function1;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class QueryJob implements Callable<Void>{

    private static final Logger LOG = Logger.getLogger(QueryJob.class);

    private final OlapStatus status;
    private final RemoteQueryJob queryRequest;

    private final Clock clock;
    private final long tickTime;
    private String jobName;

    public QueryJob(RemoteQueryJob queryRequest,
                    OlapStatus jobStatus,
                    Clock clock,
                    long tickTime) {
        this.status = jobStatus;
        this.clock = clock;
        this.tickTime = tickTime;
        this.queryRequest = queryRequest;
    }

    @Override
    public Void call() throws Exception {
        if(!status.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        JavaSparkContext context=SpliceSpark.getContext();
        ActivationHolder ah = queryRequest.ah;
        SpliceOperation root = ah.getOperationsMap().get(queryRequest.rootResultSetNumber);
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        Activation activation = ah.getActivation();
        root.setActivation(ah.getActivation());
        String sql=activation.getPreparedStatement().getSource();
        if (!(activation.isMaterialized()))
            activation.materialize();
        long txnId=root.getCurrentTransaction().getTxnId();
        sql=sql==null?this.toString():sql;
        String userId=activation.getLanguageConnectionContext().getCurrentUserId(activation);
        if (dsp.getType() == DataSetProcessor.Type.SPARK) { // Only do this for spark jobs
            this.jobName = userId + " <" + txnId + ">";
            dsp.setJobGroup(jobName, sql);
        }
        dsp.clearBroadcastedOperation();
        DataSet<LocatedRow> dataset = root.getDataSet(dsp);
        SparkDataSet<LocatedRow> sparkDataSet = (SparkDataSet<LocatedRow>) dataset;
        String clientHost = queryRequest.host;
        int clientPort = queryRequest.port;
        int numPartitions = sparkDataSet.rdd.getNumPartitions();
        StreamableRDD streamableRDD = new StreamableRDD(sparkDataSet.rdd, clientHost, clientPort);

        Object result = streamableRDD.result();

//        JavaFutureAction<List<Object>> collectFuture = streamed.collectAsync();
//        while(!collectFuture.isDone()){
//            try{
//                collectFuture.get(tickTime,TimeUnit.MILLISECONDS);
//            }catch(TimeoutException te){
                /*
                 * A TimeoutException just means that tickTime expired. That's okay, we just stick our
                 * head up and make sure that the client is still operating
                 */
//            }
//            if(!status.isRunning()){
//                /*
//                 * The client timed out, so cancel the query and terminate
//                 */
//                collectFuture.cancel(true);
//                context.cancelJobGroup(queryRequest.jobGroup);
//                return null;
//            }
//        }

        //the compaction completed
//        List<Object> results = collectFuture.get();
        status.markCompleted(new QueryResult(numPartitions));

        return null;
    }

    private void sendPartitions(String host, int port, int partitions) throws InterruptedException {
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
        channel.write((Long) (long) partitions).await();
        channel.close();
        bootstrap.releaseExternalResources();
    }
}
