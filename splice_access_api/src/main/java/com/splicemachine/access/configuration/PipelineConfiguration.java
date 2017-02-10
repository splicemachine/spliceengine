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

package com.splicemachine.access.configuration;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class PipelineConfiguration implements ConfigurationDefault {
    public static final String WRITE_COORDINATOR_OBJECT_LOCATION = "com.splicemachine.writer:type=WriteCoordinatorStatus";
    public static final String WRITER_STATUS_OBJECT_LOCATION = "com.splicemachine.writer.async:type=WriterStatus";
    public static final String THREAD_POOL_STATUS_LOCATION = "com.splicemachine.writer.async:type=ThreadPoolStatus";

    public static final String MAX_BUFFER_ENTRIES= "splice.client.write.buffer.maxentries";
    public static final int DEFAULT_MAX_BUFFER_ENTRIES = 1000;

    public static final String MAX_BUFFER_HEAP_SIZE = "splice.client.write.buffer";
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 3*1024*1024;

    public static final String SPARK_IO_COMPRESSION_CODEC = "spark.io.compression.codec";
    public static final String DEFAULT_SPARK_IO_COMPRESSION_CODEC = "lz4";

    public static final String SPARK_RESULT_STREAMING_BATCHES = "spark.result.streaming.batches";
    public static final int DEFAULT_SPARK_RESULT_STREAMING_BATCHES = 10;

    public static final String SPARK_RESULT_STREAMING_BATCH_SIZE = "spark.result.streaming.batch.size";
    public static final int DEFAULT_SPARK_RESULT_STREAMING_BATCH_SIZE = 1024;

    public static final String SPARK_COMPACTION_RESERVED_SLOTS = "spark.compaction.reserved.slots";
    public static final int DEFAULT_SPARK_COMPACTION_RESERVED_SLOTS = 1;

    // Timeout in milliseconds
    public static final String SPARK_COMPACTION_MAXIMUM_WAIT = "spark.compaction.maximum.wait";
    public static final int DEFAULT_SPARK_COMPACTION_MAXIMUM_WAIT = 60000;

    // Timeout in seconds
    public static final String SPARK_RESERVED_SLOTS_TIMEOUT = "spark.reserved.slots.timeout";
    public static final int DEFAULT_SPARK_RESERVED_SLOTS_TIMEOUT = 60;

    /**
     * The number of times to retry a network operation before failing.  Turning this up will reduce the number of spurious
     * failures caused by network events (NotServingRegionException, IndexNotSetUpException, etc.), but will also lengthen
     * the time taken by a query before a failure is detected. Turning this down will decrease the latency required
     * before detecting a failure, but may result in more spurious failures (especially during large writes).
     *
     * Generally, the order of retries is as follows:
     *
     * try 1, pause, try 2, pause, try 3, pause,try 4, 2*pause, try 5, 2*pause, try 6, 4*pause, try 7, 4*pause,
     * try 8, 8*pause, try 9, 16*pause, try 10, 32*pause,try 11, 32*pause,...try {max}, fail
     *
     * So if the pause time (hbase.client.pause) is set to 1 second, and the number of retries is 10, the total time
     * before a write can fail is 71 seconds. If the number of retries is 5, then the total time before failing is
     * 5 seconds. If the number of retries is 20, the total time before failing is 551 seconds(approximately 10 minutes).
     *
     * It is recommended to turn this setting up if you are seeing a large number of operations failing with
     * NotServingRegionException, WrongRegionException, or IndexNotSetUpException errors, or if it is known
     * that the mean time to recovery of a single region is longer than the total time before failure.
     *
     * Defaults to 10.
     */
    public static final String MAX_RETRIES = "hbase.client.retries.number";
    public static final int DEFAULT_HBASE_CLIENT_RETRIES_NUMBER = 31;

    public static final String CLIENT_PAUSE = "splice.client.pause";
    public static final long DEFAULT_CLIENT_PAUSE = 1000;

    /**
     * The maximum number of concurrent buffer flushes that are allowed to be directed to a single
     * region by a single write operation. This helps to prevent overloading an individual region,
     * at the cost of reducing overall throughput to that region.
     *
     * Turn this setting down if you encounter an excessive number of RegionTooBusyExceptions. Turn
     * this setting up if system load is lower than expected during large writes, and the number of write
     * threads are not fully utilized.
     *
     * This setting becomes useless once set higher than the maximum number of write threads (splice.writer.maxThreads),
     * as a single region can never allocate more than the maximum total number of write threads.
     *
     * Defaults to 5
     */
    public static final String WRITE_MAX_FLUSHES_PER_REGION = "splice.writer.maxFlushesPerRegion";
    public static final int WRITE_DEFAULT_MAX_FLUSHES_PER_REGION = 5;

    /**
     * The amount of time (in milliseconds) to wait during index initialization before
     * forcing a write to return. This setting prevents deadlocks during startup in small clusters,
     * and is also the source of IndexNotSetUpExceptions.
     *
     * If an excessively high number of IndexNotSetUpExceptions are being seen, consider increasing
     * this setting. However, if set too high, this may result in deadlocks on small clusters.
     *
     * Defaults to 1000 ms (1 s)
     */
    public static final String STARTUP_LOCK_WAIT_PERIOD = "splice.startup.lockWaitPeriod";
    public static final long DEFAULT_STARTUP_LOCK_PERIOD=1000;

    /**
     * The maximum number of threads which may be used to concurrently write data to any HBase table.
     * In order to prevent potential deadlock situations, this parameter cannot be higher than the
     * number of available IPC threads (hbase.regionserver.handler.count); setting the max threads
     * to a number higher than the available IPC threads will have no effect.
     *
     * This parameter may be adjusted in real time using JMX.
     *
     * Default is 5.
     */
    public static final String MAX_WRITER_THREADS= "splice.writer.maxThreads";
    public static final int DEFAULT_MAX_WRITER_THREADS= 5;

    /**
     * The number of write threads to allow to remain alive even when the maximum number of threads
     * is not required. Adjusting this only affects how quickly a write thread is allowed to proceed
     * in some cases, and the number of threads which are alive in the overall system without at any
     * given point in time. * This generally does not require adjustment, unless thread-management is
     * problematic.
     *
     * Default is 2.
     */
    public static final String CORE_WRITER_THREADS= "splice.writer.coreThreads";
    public static final int DEFAULT_WRITE_THREADS_CORE = 2;

    /**
     * The length of time (in seconds) to wait before killing a write thread which is not in use. Turning
     * this up will result in more threads being available for writes after longer periods of inactivity,
     * but will cause higher thread counts in the system overall. Turning this down will result in fewer
     * threads being maintained in the system at any given point in time, but will also require more
     * thread startups (potentially affecting performance). This generally does not require adjustment,
     * unless thread-management is problematic or context switching is knowng to be an issue.
     *
     * Default is 60 seconds.
     */
    public static final String THREAD_KEEPALIVE_TIME= "hbase.htable.threads.keepalivetime";
    public static final long DEFAULT_THREAD_KEEPALIVE_TIME= 60;

    public static final String MAX_INDEPENDENT_WRITES = "splice.client.write.maxIndependentWrites";
    public static final int DEFAULT_MAX_INDEPENDENT_WRITES = 40000;

    public static final String MAX_DEPENDENT_WRITES = "splice.client.write.maxDependentWrites";
    public static final int DEFAULT_MAX_DEPENDENT_WRITES = 40000;

    public static final String IPC_THREADS="hbase.regionserver.handler.count";
    public static final int DEFAULT_IPC_THREADS = 200;

    public static final String PIPELINE_KRYO_POOL_SIZE= "splice.writer.kryoPoolSize";
    private static final int DEFAULT_PIPELINE_KRYO_POOL_SIZE=1024;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.ipcThreads = configurationSource.getInt(IPC_THREADS, DEFAULT_IPC_THREADS);
        builder.maxIndependentWrites = configurationSource.getInt(MAX_INDEPENDENT_WRITES, DEFAULT_MAX_INDEPENDENT_WRITES);
        builder.maxDependentWrites = configurationSource.getInt(MAX_DEPENDENT_WRITES, DEFAULT_MAX_DEPENDENT_WRITES);
        builder.coreWriterThreads = configurationSource.getInt(CORE_WRITER_THREADS, DEFAULT_WRITE_THREADS_CORE);
        builder.maxWriterThreads = configurationSource.getInt(MAX_WRITER_THREADS, DEFAULT_MAX_WRITER_THREADS);
        builder.writeMaxFlushesPerRegion = configurationSource.getInt(WRITE_MAX_FLUSHES_PER_REGION, WRITE_DEFAULT_MAX_FLUSHES_PER_REGION);
        builder.maxRetries = configurationSource.getInt(MAX_RETRIES, DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        builder.maxBufferEntries = configurationSource.getInt(MAX_BUFFER_ENTRIES, DEFAULT_MAX_BUFFER_ENTRIES);
        builder.pipelineKryoPoolSize = configurationSource.getInt(PIPELINE_KRYO_POOL_SIZE, DEFAULT_PIPELINE_KRYO_POOL_SIZE);

        builder.threadKeepaliveTime = configurationSource.getLong(THREAD_KEEPALIVE_TIME, DEFAULT_THREAD_KEEPALIVE_TIME);
        builder.clientPause = configurationSource.getLong(CLIENT_PAUSE, DEFAULT_CLIENT_PAUSE);
        builder.maxBufferHeapSize = configurationSource.getLong(MAX_BUFFER_HEAP_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
        builder.startupLockWaitPeriod = configurationSource.getLong(STARTUP_LOCK_WAIT_PERIOD, DEFAULT_STARTUP_LOCK_PERIOD);

        builder.sparkIoCompressionCodec = configurationSource.getString(SPARK_IO_COMPRESSION_CODEC, DEFAULT_SPARK_IO_COMPRESSION_CODEC);
        builder.sparkResultStreamingBatches = configurationSource.getInt(SPARK_RESULT_STREAMING_BATCHES, DEFAULT_SPARK_RESULT_STREAMING_BATCHES);
        builder.sparkResultStreamingBatchSize = configurationSource.getInt(SPARK_RESULT_STREAMING_BATCH_SIZE, DEFAULT_SPARK_RESULT_STREAMING_BATCH_SIZE);
        builder.compactionReservedSlots = configurationSource.getInt(SPARK_COMPACTION_RESERVED_SLOTS, DEFAULT_SPARK_COMPACTION_RESERVED_SLOTS);
        builder.olapCompactionMaximumWait = configurationSource.getInt(SPARK_COMPACTION_MAXIMUM_WAIT, DEFAULT_SPARK_COMPACTION_MAXIMUM_WAIT);
        builder.reservedSlotsTimeout = configurationSource.getInt(SPARK_RESERVED_SLOTS_TIMEOUT, DEFAULT_SPARK_RESERVED_SLOTS_TIMEOUT);
    }
}
