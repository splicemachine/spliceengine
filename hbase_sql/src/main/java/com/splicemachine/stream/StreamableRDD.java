/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDD<T> {
    private static final Logger LOG = Logger.getLogger(StreamableRDD.class);
    public static int PARALLEL_PARTITIONS = 4;

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


    StreamableRDD(JavaRDD<T> rdd, UUID uuid, String clientHost, int clientPort) {
        this(rdd, null, uuid, clientHost, clientPort, 2, 512);
    }

    public StreamableRDD(JavaRDD<T> rdd, OperationContext<?> context, UUID uuid, String clientHost, int clientPort, int batches, int batchSize) {
        this.rdd = rdd;
        this.context = context;
        this.uuid = uuid;
        this.host = clientHost;
        this.port = clientPort;
        this.executor = Executors.newFixedThreadPool(PARALLEL_PARTITIONS);
        completionService = new ExecutorCompletionService<>(executor);
        this.clientBatchSize = batchSize;
        this.clientBatches = batches;
    }

    public void submit() throws Exception {
        Exception error = null;
        try {
            final JavaRDD<String> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(context, uuid, host, port, rdd.getNumPartitions(), clientBatches, clientBatchSize), true);
            int numPartitions = streamed.getNumPartitions();
            int partitionsBatchSize = PARALLEL_PARTITIONS / 2;
            int partitionBatches = numPartitions / partitionsBatchSize;
            if (numPartitions % partitionsBatchSize > 0)
                partitionBatches++;

            if (LOG.isTraceEnabled())
                LOG.trace("Num partitions " + numPartitions + " clientBatches " + partitionBatches);

            Properties properties = SpliceSpark.getContext().sc().getLocalProperties();

            submitBatch(0, partitionsBatchSize, numPartitions, streamed, properties);
            if (partitionBatches > 1)
                submitBatch(1, partitionsBatchSize, numPartitions, streamed, properties);

            int received = 0;
            int submitted = 2;
            while (received < partitionBatches && error == null) {
                Future<Object> resultFuture = null;
                try {
                    resultFuture = completionService.take();
                    Object result = resultFuture.get();
                    received++;
                    if ("STOP".equals(result)) {
                        if (LOG.isTraceEnabled())
                            LOG.trace("Stopping after receiving " + received + " clientBatches of " + partitionBatches);
                        break;
                    }
                    if (submitted < partitionBatches) {
                        submitBatch(submitted, partitionsBatchSize, numPartitions, streamed, properties);
                        submitted++;
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
            public Object call() {
                SpliceSpark.getContext().sc().setLocalProperties(properties);
                String[] results = (String[]) SpliceSpark.getContext().sc().runJob(streamed.rdd(), new FunctionAdapter(), objects, tag);
                for (String o2: results) {
                    if ("STOP".equals(o2)) {
                        return "STOP";
                    }
                }
                return "CONTINUE";
            }
        });
    }

}
