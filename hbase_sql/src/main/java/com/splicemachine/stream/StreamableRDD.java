package com.splicemachine.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.pipeline.Exceptions;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;
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


    StreamableRDD(JavaRDD<T> rdd, UUID uuid, String clientHost, int clientPort) {
        this(rdd, uuid, clientHost, clientPort, 2, 512);
    }

    public StreamableRDD(JavaRDD<T> rdd, UUID uuid, String clientHost, int clientPort, int batches, int batchSize) {
        this.rdd = rdd;
        this.uuid = uuid;
        this.host = clientHost;
        this.port = clientPort;
        this.executor = Executors.newFixedThreadPool(PARALLEL_PARTITIONS);
        completionService = new ExecutorCompletionService<>(executor);
        this.clientBatchSize = batchSize;
        this.clientBatches = batches;
    }

    public Object result() throws StandardException {
        final JavaRDD<String> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(uuid, host, port, rdd.getNumPartitions(), clientBatches, clientBatchSize), true);
        int numPartitions = streamed.getNumPartitions();
        int partitionsBatchSize = PARALLEL_PARTITIONS / 2;
        int partitionBatches = numPartitions / partitionsBatchSize;
        if (numPartitions % partitionsBatchSize > 0)
            partitionBatches++;

        LOG.trace("Num partitions " + numPartitions + " clientBatches " + partitionBatches);

        submitBatch(0, partitionsBatchSize, numPartitions, streamed);
        if (partitionBatches > 1)
            submitBatch(1, partitionsBatchSize, numPartitions, streamed);

        int received = 0;
        int submitted = 2;
        Exception error = null;
        while(received < partitionBatches && error == null) {
            Future<Object> resultFuture = null;
            try {
                resultFuture = completionService.take();
                Object result = resultFuture.get();
                received++;
                if ("STOP".equals(result)) {
                    LOG.trace("Stopping after receiving " + received + " clientBatches of " + partitionBatches);
                    break;
                }
                if (submitted < partitionBatches) {
                    submitBatch(submitted, partitionsBatchSize, numPartitions, streamed);
                    submitted++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch(Exception e) {
                error = e;
            }
        }
        executor.shutdown();

        if (error != null) {
            LOG.error(error);
            throw Exceptions.parseException(error);
        }

        return null;
    }

    private void submitBatch(int batch, int batchSize, int numPartitions, final JavaRDD<String> streamed) {
        final List<Integer> list = new ArrayList<>();
        for (int j = batch*batchSize; j < numPartitions && j < (batch+1)*batchSize; j++) {
            list.add(j);
        }
        LOG.trace("Submitting batch " + batch + " with partitions " + list);
        final Seq objects = JavaConversions.asScalaBuffer(list).toList();
        completionService.submit(new Callable<Object>() {
            @Override
            public Object call() {
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
