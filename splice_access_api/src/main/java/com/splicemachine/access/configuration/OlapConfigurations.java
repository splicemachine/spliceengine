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
 *
 */

package com.splicemachine.access.configuration;


import splice.com.google.common.base.Splitter;

import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Repository for holding configuration keys for Olap client/server.
 * <p/>
 * Each specific architecture configuration should provide
 * a default value for each of these keys.
 */
public class OlapConfigurations implements ConfigurationDefault {

    /**
     * The number of milliseconds the OLAP client should wait for a result.
     * Defaults to Integer.MAX_VALUE (wait forever)
     */
    public static final String OLAP_CLIENT_WAIT_TIME = "splice.olap_server.clientWaitTime";
    private static final int DEFAULT_OLAP_CLIENT_WAIT_TIME = Integer.MAX_VALUE;

    /**
     * The number of milliseconds the OLAP client should wait for performing a status check.
     * Defaults to 1000 (1 s)
     */
    public static final String OLAP_CLIENT_TICK_TIME = "splice.olap_server.clientTickTime";
    private static final int DEFAULT_OLAP_CLIENT_TICK_TIME = 1000;

    /**
     * The Port to bind the OLAP Server connection to
     * Defaults to 60014
     */
    public static final String OLAP_SERVER_BIND_PORT = "splice.olap_server.port";
    private static final int DEFAULT_OLAP_SERVER_BIND_PORT = 60014;

    /**
     * Number of threads used by the Olap server, determines the maximum number of concurrent
     * Olap jobs
     *
     * Defaults to 16
     */
    public static final String OLAP_SERVER_THREADS = "splice.olap_server.threads";
    private static final int DEFAULT_OLAP_SERVER_THREADS = 16;

    public static final String OLAP_SERVER_TICK_LIMIT = "splice.olap_server.tickLimit";
    private static final int DEFAULT_OLAP_SERVER_TICK_LIMIT = 120;

    public static final String OLAP_CLIENT_RETRIES = "splice.olap_client.retries";
    private static final int DEFAULT_OLAP_CLIENT_RETRIES = 10;

    public static final String OLAP_SHUFFLE_PARTITIONS = "splice.olap.shuffle.partitions";
    private static final int DEFAULT_OLAP_SHUFFLE_PARTITIONS = 200;

    /**
     * The directory to use for staging files sent to the Olap Server
     * Defaults to NULL
     */
    public static final String OLAP_SERVER_STAGING_DIR = "splice.olap_server.stagingDirectory";
    private static final String DEFAULT_OLAP_SERVER_STAGING_DIR = null;

    /**
     * Run OlapServer externally on YARN
     * Defaults to true
     */
    public static final String OLAP_SERVER_EXTERNAL = "splice.olap_server.external";
    private static final boolean DEFAULT_OLAP_SERVER_EXTERNAL = true;

    public static final String OLAP_SERVER_MAX_RETRIES = "splice.olap_server.maxRetries";
    public static final int DEFAULT_OLAP_SERVER_MAX_RETRIES = 30;

    public static final String OLAP_SERVER_SUBMIT_ATTEMPTS = "splice.olap_server.submitAttempts";
    private static final int DEFAULT_OLAP_SERVER_SUBMIT_ATTEMPTS = 50;

    public static final String OLAP_SERVER_MEMORY = "splice.olap_server.memory";
    private static final int DEFAULT_OLAP_SERVER_MEMORY = 1024;

    public static final String OLAP_SERVER_MEMORY_OVERHEAD = "splice.olap_server.memoryOverhead";
    private static final int DEFAULT_OLAP_SERVER_MEMORY_OVERHEAD = 512;

    public static final String OLAP_SERVER_VIRTUAL_CORES = "splice.olap_server.virtualCores";
    private static final int DEFAULT_OLAP_SERVER_VIRTUAL_CORES = 1;

    // Timeout in milliseconds
    public static final String SPARK_COMPACTION_MAXIMUM_WAIT = "spark.compaction.maximum.wait";
    public static final int DEFAULT_SPARK_COMPACTION_MAXIMUM_WAIT = 60000;

    // Maximum concurrent compactions
    public static final String SPARK_COMPACTION_MAXIMUM_CONCURRENT = "spark.compaction.maximum.concurrent";
    public static final int DEFAULT_SPARK_COMPACTION_MAXIMUM_CONCURRENT = Integer.MAX_VALUE;

    // Share of time spent on transaction resolution, between 0 and 1 (no time vs infinite time)
    public static final String SPARK_COMPACTION_RESOLUTION_SHARE = "spark.compaction.resolution.share";
    public static final double DEFAULT_SPARK_COMPACTION_RESOLUTION_SHARE = 0.2f;
    public static final double MIN_SPARK_COMPACTION_RESOLUTION_SHARE = 0;
    public static final double MAX_SPARK_COMPACTION_RESOLUTION_SHARE = 1;

    // Size of buffer for asynchronous transaction resolution
    public static final String SPARK_COMPACTION_RESOLUTION_BUFFER_SIZE = "spark.compaction.resolution.bufferSize";
    public static final int DEFAULT_SPARK_COMPACTION_RESOLUTION_BUFFER_SIZE = 1024*1024;

    // Whether we block asynchronous transaction resolution when the executor is full
    public static final String SPARK_COMPACTION_BLOCKING = "spark.compaction.blocking";
    public static final boolean DEFAULT_SPARK_COMPACTION_BLOCKING = true;

    // Whether we record execution stats on Spark accumulators
    public static final String SPARK_ACCUMULATORS_ENABLED = "spark.accumulators.enabled";
    public static final boolean DEFAULT_SPARK_ACCUMULATORS_ENABLED = true;

    // Log4j config file for OLAP server
    public static final String OLAP_LOG4J_CONFIG = "splice.olap.log4j.configuration";
    public static final String DEFAULT_OLAP_LOG4J_CONFIG = null;

    /* Map of roles to Splice queue names. Role names are case sensitive, typically they are all uppercase

    Examples:
        ISOLATEDROLE=customQueue
        ROLE1=queue1,role2=queue2
     */
    public static final String OLAP_SERVER_ISOLATED_ROLES = "splice.olap_server.isolated.roles";
    public static final String DEFAULT_OLAP_SERVER_ISOLATED_ROLES = "";

    public static final String OLAP_SERVER_YARN_DEFAULT_QUEUE = "splice.olap_server.queue.default";
    public static final String DEFAULT_OLAP_SERVER_YARN_DEFAULT_QUEUE = "default";

    // Whether we use a dedicated compaction queue
    public static final String OLAP_SERVER_ISOLATED_COMPACTION = "splice.olap_server.isolated.compaction";
    public static final boolean DEFAULT_OLAP_SERVER_ISOLATED_COMPACTION = false;

    // Specify name for dedicated compaction queue, if applicable
    public static final String OLAP_SERVER_ISOLATED_COMPACTION_QUEUE_NAME = "splice.olap_server.isolated.compaction.queue_name";
    public static final String DEFAULT_OLAP_SERVER_ISOLATED_COMPACTION_QUEUE_NAME = "compaction";

    // Whether we should purge deleted rows during flush & compaction
    public static final String OLAP_COMPACTION_AUTOMATICALLY_PURGE_DELETED_ROWS = "splice.olap.compaction.automaticallyPurgeDeletedRows";
    public static final boolean DEFAULT_OLAP_COMPACTION_AUTOMATICALLY_PURGE_DELETED_ROWS = true;

    // Whether we should purge old updates during flush & compaction
    public static final String OLAP_COMPACTION_AUTOMATICALLY_PURGE_OLD_UPDATES = "splice.olap.compaction.automaticallyPurgeOldUpdates";
    public static final boolean DEFAULT_OLAP_COMPACTION_AUTOMATICALLY_PURGE_OLD_UPDATES = false;

    // Olap Server keepalive timeout in seconds until it kills itself. It has to be larger than an HMaster failover
    // when deployed on premise if we want it to survive the HMaster failover.
    public static final String OLAP_SERVER_KEEPALIVE_TIMEOUT = "splice.olap.server.keepalive.timeout";
    public static final long DEFAULT_OLAP_SERVER_KEEPALIVE_TIMEOUT = 900; // 15 minutes

    // Olap Server mode for external execution, either YARN or KUBERNETES
    public static final String OLAP_SERVER_MODE = "splice.olap_server.deployment.mode";
    public static final String DEFAULT_OLAP_SERVER_MODE = "YARN";

    /* Map of Splice queues to YARN queues

    Examples:
       splice.olap_server.queue.customQueue=project1
       splice.olap_server.queue.queue1=yarnQueue1
       splice.olap_server.queue.queue2=yarnQueue2
    */
    public static final String OLAP_SERVER_YARN_QUEUES = "splice.olap_server.queue.";

    public static final String SPARK_IO_COMPRESSION_CODEC = "spark.io.compression.codec";
    public static final String DEFAULT_SPARK_IO_COMPRESSION_CODEC = "lz4";

    public static final String SPARK_RESULT_STREAMING_BATCHES = "spark.result.streaming.batches";
    public static final int DEFAULT_SPARK_RESULT_STREAMING_BATCHES = 10;

    public static final String SPARK_RESULT_STREAMING_BATCH_SIZE = "spark.result.streaming.batch.size";
    public static final int DEFAULT_SPARK_RESULT_STREAMING_BATCH_SIZE = 1024;

    public static final String SPARK_SLOW_RESULT_STREAMING_BATCHES = "spark.slow.result.streaming.batches";
    public static final int DEFAULT_SPARK_SLOW_RESULT_STREAMING_BATCHES = 4;

    public static final String SPARK_SLOW_RESULT_STREAMING_BATCH_SIZE = "spark.slow.result.streaming.batch.size";
    public static final int DEFAULT_SPARK_SLOW_RESULT_STREAMING_BATCH_SIZE = 20;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.olapServerBindPort  = configurationSource.getInt(OLAP_SERVER_BIND_PORT, DEFAULT_OLAP_SERVER_BIND_PORT);
        builder.olapServerStagingDir = configurationSource.getString(OLAP_SERVER_STAGING_DIR, DEFAULT_OLAP_SERVER_STAGING_DIR);
        builder.olapServerExternal  = configurationSource.getBoolean(OLAP_SERVER_EXTERNAL, DEFAULT_OLAP_SERVER_EXTERNAL);
        builder.olapServerMaxRetries = configurationSource.getInt(OLAP_SERVER_MAX_RETRIES, DEFAULT_OLAP_SERVER_MAX_RETRIES);
        builder.olapClientWaitTime  = configurationSource.getInt(OLAP_CLIENT_WAIT_TIME, DEFAULT_OLAP_CLIENT_WAIT_TIME);
        builder.olapClientTickTime  = configurationSource.getInt(OLAP_CLIENT_TICK_TIME, DEFAULT_OLAP_CLIENT_TICK_TIME);
        builder.olapServerThreads = configurationSource.getInt(OLAP_SERVER_THREADS, DEFAULT_OLAP_SERVER_THREADS);
        builder.olapServerTickLimit = configurationSource.getInt(OLAP_SERVER_TICK_LIMIT,DEFAULT_OLAP_SERVER_TICK_LIMIT);
        builder.olapClientRetries = configurationSource.getInt(OLAP_CLIENT_RETRIES,DEFAULT_OLAP_CLIENT_RETRIES);
        builder.olapServerSubmitAttempts = configurationSource.getInt(OLAP_SERVER_SUBMIT_ATTEMPTS, DEFAULT_OLAP_SERVER_SUBMIT_ATTEMPTS);
        builder.olapServerMemory = configurationSource.getInt(OLAP_SERVER_MEMORY, DEFAULT_OLAP_SERVER_MEMORY);
        builder.olapServerMemoryOverhead = configurationSource.getInt(OLAP_SERVER_MEMORY_OVERHEAD, DEFAULT_OLAP_SERVER_MEMORY_OVERHEAD);
        builder.olapServerVirtualCores = configurationSource.getInt(OLAP_SERVER_VIRTUAL_CORES, DEFAULT_OLAP_SERVER_VIRTUAL_CORES);
        builder.olapShufflePartitions = configurationSource.getInt(OLAP_SHUFFLE_PARTITIONS,DEFAULT_OLAP_SHUFFLE_PARTITIONS);
        builder.olapCompactionMaximumWait = configurationSource.getInt(SPARK_COMPACTION_MAXIMUM_WAIT, DEFAULT_SPARK_COMPACTION_MAXIMUM_WAIT);
        builder.olapCompactionMaximumConcurrent = configurationSource.getInt(SPARK_COMPACTION_MAXIMUM_CONCURRENT, DEFAULT_SPARK_COMPACTION_MAXIMUM_CONCURRENT);
        builder.olapCompactionResolutionShare = configurationSource.getDouble(SPARK_COMPACTION_RESOLUTION_SHARE, DEFAULT_SPARK_COMPACTION_RESOLUTION_SHARE, MIN_SPARK_COMPACTION_RESOLUTION_SHARE, MAX_SPARK_COMPACTION_RESOLUTION_SHARE);

        builder.olapCompactionResolutionBufferSize = configurationSource.getInt(SPARK_COMPACTION_RESOLUTION_BUFFER_SIZE, DEFAULT_SPARK_COMPACTION_RESOLUTION_BUFFER_SIZE);
        builder.olapCompactionBlocking = configurationSource.getBoolean(SPARK_COMPACTION_BLOCKING, DEFAULT_SPARK_COMPACTION_BLOCKING);
        builder.olapLog4jConfig = configurationSource.getString(OLAP_LOG4J_CONFIG, DEFAULT_OLAP_LOG4J_CONFIG);
        String isolatedRoles = configurationSource.getString(OLAP_SERVER_ISOLATED_ROLES, DEFAULT_OLAP_SERVER_ISOLATED_ROLES);
        builder.olapServerIsolatedRoles = isolatedRoles.isEmpty() ? Collections.emptyMap() : Splitter.on(",")
                .withKeyValueSeparator("=")
                .split(isolatedRoles);

        builder.olapServerIsolatedCompaction = configurationSource.getBoolean(OLAP_SERVER_ISOLATED_COMPACTION, DEFAULT_OLAP_SERVER_ISOLATED_COMPACTION);
        builder.olapServerIsolatedCompactionQueueName = configurationSource.getString(OLAP_SERVER_ISOLATED_COMPACTION_QUEUE_NAME, DEFAULT_OLAP_SERVER_ISOLATED_COMPACTION_QUEUE_NAME);

        // Set up mappings for Splice Queues to YARN queues, possible mappings are all defined in olapServerIsolatedRoles,
        // the compaction queue name if enabled and the 'default' queue
        Stream<String> queueNames = Stream.concat(builder.olapServerIsolatedRoles.values().stream(), Stream.of("default"));
        if (builder.olapServerIsolatedCompaction)
            queueNames = Stream.concat(queueNames, Stream.of(builder.olapServerIsolatedCompactionQueueName));
        builder.olapServerYarnQueues = queueNames.collect(Collectors.toMap(
                        Function.identity(),
                        queue -> configurationSource.getString(OLAP_SERVER_YARN_QUEUES + queue, DEFAULT_OLAP_SERVER_YARN_DEFAULT_QUEUE)));

        builder.olapCompactionAutomaticallyPurgeDeletedRows = configurationSource.getBoolean(OLAP_COMPACTION_AUTOMATICALLY_PURGE_DELETED_ROWS, DEFAULT_OLAP_COMPACTION_AUTOMATICALLY_PURGE_DELETED_ROWS);
        builder.olapCompactionAutomaticallyPurgeOldUpdates = configurationSource.getBoolean(OLAP_COMPACTION_AUTOMATICALLY_PURGE_OLD_UPDATES, DEFAULT_OLAP_COMPACTION_AUTOMATICALLY_PURGE_OLD_UPDATES);

        builder.sparkIoCompressionCodec = configurationSource.getString(SPARK_IO_COMPRESSION_CODEC, DEFAULT_SPARK_IO_COMPRESSION_CODEC);
        builder.sparkResultStreamingBatches = configurationSource.getInt(SPARK_RESULT_STREAMING_BATCHES, DEFAULT_SPARK_RESULT_STREAMING_BATCHES);
        builder.sparkResultStreamingBatchSize = configurationSource.getInt(SPARK_RESULT_STREAMING_BATCH_SIZE, DEFAULT_SPARK_RESULT_STREAMING_BATCH_SIZE);
        builder.sparkSlowResultStreamingBatches = configurationSource.getInt(SPARK_SLOW_RESULT_STREAMING_BATCHES, DEFAULT_SPARK_SLOW_RESULT_STREAMING_BATCHES);
        builder.sparkSlowResultStreamingBatchSize = configurationSource.getInt(SPARK_SLOW_RESULT_STREAMING_BATCH_SIZE, DEFAULT_SPARK_SLOW_RESULT_STREAMING_BATCH_SIZE);

        builder.olapServerKeepAliveTimeout = configurationSource.getLong(OLAP_SERVER_KEEPALIVE_TIMEOUT, DEFAULT_OLAP_SERVER_KEEPALIVE_TIMEOUT);
        builder.olapServerMode = configurationSource.getString(OLAP_SERVER_MODE, DEFAULT_OLAP_SERVER_MODE);
        builder.sparkAccumulatorsEnabled = configurationSource.getBoolean(SPARK_ACCUMULATORS_ENABLED, DEFAULT_SPARK_ACCUMULATORS_ENABLED);
    }
}
