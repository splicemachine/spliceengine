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
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDD<T> {
    private static final Logger LOG = Logger.getLogger(StreamableRDD.class);
    public static int PARALLEL_PARTITIONS = 4;

    private static final ClassTag<Object[]> tag = scala.reflect.ClassTag$.MODULE$.apply(Object[].class);
    private final int port;
    private final String host;
    private final JavaRDD<T> rdd;
    private final ExecutorCompletionService<Void> completionService;

    public StreamableRDD(JavaRDD<T> rdd, String clientHost, int clientPort) {
        this.rdd = rdd;
        this.host = clientHost;
        this.port = clientPort;
        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_PARTITIONS);
        completionService = new ExecutorCompletionService<>(executor);
    }

    public Object result() throws StandardException {
        final JavaRDD<Object> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(host, port, rdd.getNumPartitions()), true);
        int numPartitions = streamed.getNumPartitions();
        int batchSize = PARALLEL_PARTITIONS / 2;
        int batches = numPartitions / batchSize;
        if (numPartitions % batchSize > 0)
            batches++;

        LOG.trace("Num partitions " + numPartitions);

        submitBatch(0, batchSize, numPartitions, streamed);
        if (batches > 1)
            submitBatch(1, batchSize, numPartitions, streamed);

        int received = 0;
        int submitted = 2;
        Exception error = null;
        while(received < batches && error == null) {
            Future<Void> resultFuture = null;
            try {
                resultFuture = completionService.take();
                resultFuture.get();
                received++;
                if (submitted < batches) {
                    submitBatch(submitted, batchSize, numPartitions, streamed);
                    submitted++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch(Exception e) {
                error = e;
            }
        }

        if (error != null) {
            throw Exceptions.parseException(error);
        }

        return null;
    }

    private void submitBatch(int batch, int batchSize, int numPartitions, final JavaRDD<Object> streamed) {
        final List<Integer> list = new ArrayList<>();
        for (int j = batch*batchSize; j < numPartitions && j < (batch+1)*batchSize; j++) {
            list.add(j);
        }
        LOG.warn("Submitting partitions " + list);
        final Seq objects = JavaConversions.asScalaBuffer(list).toList();
        completionService.submit(new Runnable() {
            @Override
            public void run() {
                LOG.trace("Running partitions " + list);
                SpliceSpark.getContext().sc().runJob(streamed.rdd(), new FunctionAdapter(), objects, tag);
            }
        }, null);
    }

}
