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

import com.splicemachine.access.configuration.OlapConfigurations;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SimpleFutureAction;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.max;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDD<T> {
    private static final Logger LOG = Logger.getLogger(StreamableRDD.class);
    public static final int DEFAULT_PARALLEL_PARTITIONS = 4;
    public static final int MAX_PARALLEL_THREADS = 8;

    private static final ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
    private final int port;
    private final int clientBatchSize;
    private final String host;
    private final JavaRDD<T> rdd;
    private final ExecutorCompletionService<Object> completionService;
    private final ExecutorService executor;
    private final int clientBatches;
    private final UUID uuid;
    private final OperationContext<?> context;
    private final int parallelPartitions;
    private OlapStatus jobStatus;
    private OlapStreamListener olapStreamListener;
    private final int executorThreads;


    StreamableRDD(JavaRDD<T> rdd, UUID uuid, String clientHost, int clientPort) {
        this(rdd, null, uuid, clientHost, clientPort, 2, 512);
    }

    public StreamableRDD(JavaRDD<T> rdd, OperationContext<?> context, UUID uuid, String clientHost, int clientPort, int batches, int batchSize) {
        this(rdd, context, uuid, clientHost, clientPort, batches, batchSize, DEFAULT_PARALLEL_PARTITIONS, OlapConfigurations.DEFAULT_SPARK_RESULT_STREAMING_THREADS);
    }


    public StreamableRDD(JavaRDD<T> rdd, OperationContext<?> context, UUID uuid, String clientHost, int clientPort,
                         int batches, int batchSize, int parallelPartitions, int executorThreads) {
        this.rdd = rdd;
        this.context = context;
        this.uuid = uuid;
        this.host = clientHost;
        this.port = clientPort;
        int defaultExecutorThreads = (int) max(min(ceil((float)parallelPartitions / 2), StreamableRDD.MAX_PARALLEL_THREADS),
                OlapConfigurations.DEFAULT_SPARK_RESULT_STREAMING_THREADS);
        this.executorThreads = max(defaultExecutorThreads, executorThreads);
        this.parallelPartitions = this.executorThreads < parallelPartitions ? parallelPartitions : this.executorThreads;
        this.executor = Executors.newFixedThreadPool(this.executorThreads);
        completionService = new ExecutorCompletionService<>(executor);
        this.clientBatchSize = batchSize;
        this.clientBatches = batches;
        this.olapStreamListener = new OlapStreamListener(host, port, uuid);
    }

    public void submit() throws Exception {
        Exception error = null;
        olapStreamListener.createChannelToStreamListener();
        try {
            final JavaRDD<String> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(context, uuid, host, port, rdd.getNumPartitions(), clientBatches, clientBatchSize), true);
            int numPartitions = streamed.getNumPartitions();
            int partitionsBatchSize = (int) ceil((float)parallelPartitions / executorThreads);
            int partitionBatches = (int) ceil((float)numPartitions / partitionsBatchSize);

            if (LOG.isTraceEnabled())
                LOG.trace("Num partitions " + numPartitions + " clientBatches " + partitionBatches);

            // Assume this will always be called either from testing or from the OlapServer, since those are the only
            // places this is used
            Properties properties = SerializationUtils.clone(SpliceSpark.getContextUnsafe().sc().getLocalProperties());

            int received = 0;
            int submitted = 0;
            int running = 0;
            while (received < partitionBatches && error == null) {
                if (jobStatus != null && !jobStatus.isRunning()) {
                    throw new CancellationException(String.format("The olap job %s is no longer running, cancelling Spark job", this.uuid.toString()));
                }
                try {
                    if (running < this.executorThreads) {
                        if (running == 0 && !olapStreamListener.isClientConsuming()) {
                            olapStreamListener.getLock().lock();
                            try {
                                olapStreamListener.getCondition().await(10, TimeUnit.SECONDS);
                            } finally {
                                olapStreamListener.getLock().unlock();
                            }
                            continue;
                        }
                        if (olapStreamListener.isClientConsuming() && submitted < partitionBatches) {
                            submitBatch(submitted, partitionsBatchSize, numPartitions, streamed, properties);
                            submitted++;
                            running++;
                        }
                    }

                    if ((running > 0 && (!olapStreamListener.isClientConsuming() || submitted == partitionBatches)) || running == this.executorThreads) {
                        Future<Object> resultFuture = null;
                        resultFuture = completionService.poll(10, TimeUnit.SECONDS);
                        if (resultFuture == null) {
                            // retry loop checking job status
                            continue;
                        }
                        Object result = resultFuture.get();
                        running--;
                        received++;
                        if ("STOP".equals(result)) {
                            if (LOG.isTraceEnabled())
                                LOG.trace("Stopping after receiving " + received + " clientBatches of " + partitionBatches);
                            break;
                        }
                    }
                } catch (Exception e) {
                    error = e;
                }
            }
        } finally {
            executor.shutdown();
        }

        if (error != null) {
            LOG.error(error);
            throw error;
        }
    }

    private void submitBatch(int batch, int batchSize, int numPartitions, final JavaRDD<String> streamed, final Properties properties) {
        final List<Integer> list = new ArrayList<>();
        for (int j = batch*batchSize; j < numPartitions && j < (batch+1)*batchSize; j++) {
            list.add(j);
        }
        if (LOG.isTraceEnabled())
            LOG.trace("Submitting batch " + batch + " with partitions " + list);
        final Seq objects = JavaConversions.asScalaBuffer(list).toList();
        completionService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                SparkContext sc = SpliceSpark.getContextUnsafe().sc();
                sc.setLocalProperties(properties);
                AtomicBoolean cont = new AtomicBoolean(true);
                SimpleFutureAction<Boolean> job = sc.submitJob(streamed.rdd(), new FunctionAdapter(), objects, new AbstractFunction2<Object, String, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(Object index, String result) {
                        if ("STOP".equals(result))
                            cont.set(false);
                        return BoxedUnit.UNIT;
                    }
                }, new AbstractFunction0<Boolean>() {
                    @Override
                    public Boolean apply() {
                        return cont.get();
                    }
                });


                Boolean result = null;
                while (result == null) {
                    try {
                        result = Await.result(job, Duration.apply(1, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        job.cancel();
                        throw new CancellationException("Interrupted");
                    } catch (TimeoutException e) {
                        if (!jobStatus.isRunning()) {
                            job.cancel();
                            throw new CancellationException(String.format("The olap job %s is no longer running, cancelling Spark job", uuid.toString()));
                        }
                    }
                }
                return result ? "CONTINUE" : "STOP";
            }
        });
    }

    public void setJobStatus(OlapStatus jobStatus) {
        this.jobStatus = jobStatus;
    }
}
