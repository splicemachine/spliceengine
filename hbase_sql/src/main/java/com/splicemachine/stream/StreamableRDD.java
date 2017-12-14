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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDD<T> {
    private static final Logger LOG = Logger.getLogger(StreamableRDD.class);
    private static final Exception STATIC_EXCEPTION = new RuntimeException("Throwable raised by task");

    private static final ClassTag<StreamerResult> tag = scala.reflect.ClassTag$.MODULE$.apply(StreamerResult.class);
    private final int port;
    private final int clientBatchSize;
    private final String host;
    private final JavaRDD<T> rdd;
    private final ExecutorService executor;
    private final int clientBatches;
    private final UUID uuid;
    private final OperationContext<?> context;
    private final int numPartitions;
    private final int timeout;
    private Set<Integer> nextBatch = new HashSet<>();
    private AtomicInteger completedPartitionsInRound = new AtomicInteger(0);
    private volatile int completedPartitions = 0;
    private volatile boolean stop = false;
    private volatile CountDownLatch completed = new CountDownLatch(1);


    StreamableRDD(JavaRDD<T> rdd, UUID uuid, String clientHost, int clientPort) {
        this(rdd, null, uuid, clientHost, clientPort, 2, 512, 5);
    }

    public StreamableRDD(JavaRDD<T> rdd, OperationContext<?> context, UUID uuid, String clientHost, int clientPort, int batches, int batchSize, int timeout) {
        this.rdd = rdd;
        this.numPartitions = rdd.getNumPartitions();
        this.context = context;
        this.uuid = uuid;
        this.host = clientHost;
        this.port = clientPort;
        ThreadFactory tf = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ResubmissionTask-%d").build();
        this.executor = Executors.newCachedThreadPool(tf);
        this.clientBatchSize = batchSize;
        this.clientBatches = batches;
        this.timeout = timeout;
    }

        AtomicReference<Exception> error = new AtomicReference<>();
    public void submit() throws Exception {
        try {
            final JavaRDD<StreamerResult> streamed = rdd.mapPartitionsWithIndex(new ResultStreamer(context, uuid, host, port, rdd.getNumPartitions(), clientBatches, clientBatchSize, timeout), true);

            // Assume this will always be called either from testing or from the OlapServer, since those are the only
            // places this is used
            Properties properties = SpliceSpark.getContextUnsafe().sc().getLocalProperties();


            CountDownLatch completedRound = new CountDownLatch(numPartitions);

            // schedule resubmission task asynchronously, since runJob is synchronous
            executor.submit(new ResubmissionTask(completedRound, properties, streamed.rdd()));

            // submit all partitions at first, then we'll throttle if necessary
            final List<Integer> list = new ArrayList<>();
            for (int j = 0; j < numPartitions; j++) {
                list.add(j);
            }
            final Seq objects = JavaConversions.asScalaBuffer(list).toList();
            LOG.trace("Job submitted");
            SparkContext sc = SpliceSpark.getContextUnsafe().sc();
            sc.setLocalProperties(properties);
            sc.runJob(streamed.rdd(), new FunctionAdapter(), new ResultHandler(sc, completedRound), tag);

            completed.await();
        } finally {
            executor.shutdown();
        }

        if (error.get() != null) {
            LOG.error(error);
            throw error.get();
        }
    }

    class ResubmissionTask implements Runnable {
        private CountDownLatch completedRound;
        private Properties properties;
        private RDD<StreamerResult> rdd;

        public ResubmissionTask(CountDownLatch completedRound, Properties properties, RDD<StreamerResult> rdd) {
            this.completedRound = completedRound;
            this.properties = properties;
            this.rdd = rdd;
        }

        @Override
        public void run() {
            LOG.trace("Start checking results");
            try {
                completedRound.await(timeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
            if(completedPartitions >= numPartitions || stop) {
                return;
            }

            Seq partitions = null;
            synchronized (nextBatch) {
                if (!nextBatch.isEmpty()) {
                    // We have partitions to resubmit, let's throttle ourselves so we don't hog resources
                    List<Integer> list = new ArrayList<>(nextBatch);
                    Collections.sort(list);

                    // Submit at most 4 times the number of tasks completed since the last submission, up to the
                    // total number of tasks we have to resubmit
                    int completedTasks = Math.max(completedPartitionsInRound.get(), 1);
                    int maxSubmitted = Math.min(4*completedTasks, list.size());

                    list = list.subList(0, maxSubmitted);

                    if (LOG.isDebugEnabled())
                        LOG.debug("Scheduling next batch: " + list);
                    partitions = JavaConversions.asScalaBuffer(list).toList();

                    nextBatch.removeAll(list);
                    // reset count of completed tasks
                    completedPartitionsInRound.set(0);

                } else {
                    LOG.trace("Next batch is empty");
                }
            }

            // Make the blocking call outside the synchronized block
            if (partitions != null) {
                // resubmit new task for the next batch
                CountDownLatch nextRound = new CountDownLatch(partitions.size());
                executor.submit(new ResubmissionTask(nextRound, properties, rdd));
                SparkContext sc = SpliceSpark.getContextUnsafe().sc();
                sc.setLocalProperties(properties);
                try {
                    sc.runJob(rdd, new FunctionAdapter(), partitions, new ResultHandler(sc, nextRound), tag);
                } catch (Exception e) {
                    LOG.error("Spark job failed", e);
                    // Spark job failed, notify other threads
                    error.set(e);
                    completed.countDown();
                    stop = true;
                } catch (Throwable t) {
                    // Serious error, out of memory?
                    try {
                        error.set(new RuntimeException(t));
                    } catch (Throwable t2) {
                        error.set(STATIC_EXCEPTION);
                    }
                    completed.countDown();
                    stop = true;
                    LOG.error("Throwable raised", t);
                }
            } else {
                // resubmit ourselves for the inflight batch
                executor.submit(this);
            }
        }
    }

    class ResultHandler extends AbstractFunction2<Object, StreamerResult, BoxedUnit> {

        private final SparkContext sc;
        private final CountDownLatch completedRound;

        public ResultHandler(SparkContext sc, CountDownLatch completedRound) {
            this.sc = sc;
            this.completedRound = completedRound;
        }

        @Override
        public BoxedUnit apply(Object o, StreamerResult r) {
            Integer partition = r.getPartition();

            completedRound.countDown();
            switch (r.getResult()) {
                case CONTINUE:
                    completedPartitionsInRound.incrementAndGet();
                    completedPartitions++;
                    if (completedPartitions >= numPartitions)
                        completed.countDown();
                    break;
                case STUCK:
                    LOG.debug("Received STUCK from partition " + partition);
                    scheduleForNextBatch(partition);
                    break;
                case STOP:
                    LOG.debug("Received STOP after " + completedPartitions + " partitions from partition " + partition);
                    stop = true;
                    completed.countDown();
            }
            return null;
        }
    }

    private void scheduleForNextBatch(Integer partition) {
        synchronized (nextBatch) {
            nextBatch.add(partition);
        }
    }

}
