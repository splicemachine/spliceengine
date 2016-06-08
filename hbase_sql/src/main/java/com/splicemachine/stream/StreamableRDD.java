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
import java.util.concurrent.*;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDD<T> {
    private static final Logger LOG = Logger.getLogger(StreamableRDD.class);
    public static int PARALLEL_PARTITIONS = 4;

    private static final ClassTag<Object[]> tag = scala.reflect.ClassTag$.MODULE$.apply(Object[].class);
    private final int port;
    private final int batchSize;
    private final String host;
    private final JavaRDD<T> rdd;
    private final ExecutorCompletionService<Object> completionService;
    private final ExecutorService executor;
    private final int batches;


    public StreamableRDD(JavaRDD<T> rdd, String clientHost, int clientPort) {
        this(rdd, clientHost, clientPort, 2, 512);
    }

    public StreamableRDD(JavaRDD<T> rdd, String clientHost, int clientPort, int batches, int batchSize) {
        this.rdd = rdd;
        this.host = clientHost;
        this.port = clientPort;
        this.executor = Executors.newFixedThreadPool(PARALLEL_PARTITIONS);
        completionService = new ExecutorCompletionService<>(executor);
        this.batchSize = batchSize;
        this.batches = batches;
    }

    public Object result() throws StandardException {
        final JavaRDD<Object> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(host, port, rdd.getNumPartitions(), batches, batchSize), true);
        int numPartitions = streamed.getNumPartitions();
        int batchSize = PARALLEL_PARTITIONS / 2;
        int batches = numPartitions / batchSize;
        if (numPartitions % batchSize > 0)
            batches++;

        LOG.trace("Num partitions " + numPartitions + " batches " + batches);

        submitBatch(0, batchSize, numPartitions, streamed);
        if (batches > 1)
            submitBatch(1, batchSize, numPartitions, streamed);

        int received = 0;
        int submitted = 2;
        Exception error = null;
        while(received < batches && error == null) {
            Future<Object> resultFuture = null;
            try {
                resultFuture = completionService.take();
                Object result = resultFuture.get();
                received++;
                if ("STOP".equals(result)) {
                    LOG.trace("Stopping after receiving " + received + " batches of " + batches);
                    break;
                }
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
        executor.shutdown();

        if (error != null) {
            LOG.error(error);
            throw Exceptions.parseException(error);
        }

        return null;
    }

    private void submitBatch(int batch, int batchSize, int numPartitions, final JavaRDD<Object> streamed) {
        final List<Integer> list = new ArrayList<>();
        for (int j = batch*batchSize; j < numPartitions && j < (batch+1)*batchSize; j++) {
            list.add(j);
        }
        LOG.trace("Submitting batch " + batch + " with partitions " + list);
        final Seq objects = JavaConversions.asScalaBuffer(list).toList();
        completionService.submit(new Callable<Object>() {
            @Override
            public Object call() {
                Object[] results = (Object[]) SpliceSpark.getContext().sc().runJob(streamed.rdd(), new FunctionAdapter(), objects, tag);
                for (Object o : results) {
                    for (Object o2: (Object[])o) {
                        if ("STOP".equals(o2)) {
                            return "STOP";
                        }
                    }
                }
                return "CONTINUE";
            }
        });
    }

}
