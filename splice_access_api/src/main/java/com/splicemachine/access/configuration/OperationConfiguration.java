/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import com.splicemachine.primitives.Bytes;

/**
 * @author Scott Fines
 *         Date: 12/30/15
 */
public class OperationConfiguration implements ConfigurationDefault {

    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY",justification = "Intentional")
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes(SEQUENCE_TABLE_NAME);

    /**
     * The number of sequential entries to reserve in a single sequential block.
     *
     * Splice uses weakly-ordered sequential generation, in that each RegionServer will perform
     * one network operation to "reserve" a block of adjacent numbers, then it will sequentially
     * use those numbers until the block is exhausted, before fetching another block. The result
     * of which is that two different RegionServers operating concurrently with the same sequence
     * will see blocks out of order, but numbers ordered within those blocks.
     *
     * This setting configures how large those blocks may be. Turning it up will result in fewer
     * network operations during large-scale sequential id generation, and also less block-reordering
     * due to the weak-ordering. However, it will also result in a greater number of "missing" ids, since
     * a block, once allocated, can never be allocated again.
     *
     * Defaults to 1000
     */
    public static final String SEQUENCE_BLOCK_SIZE = "splice.sequence.allocationBlockSize";
    private static final int DEFAULT_SEQUENCE_BLOCK_SIZE = 1000;

    /* The maximum number of threads to be created in the general thread pool */
    public static final String THREAD_POOL_MAX_SIZE = "splice.threadPool.maxSize";
    private static final int DEFAULT_THREAD_POOL_MAX_SIZE = 256;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.sequenceBlockSize = configurationSource.getInt(SEQUENCE_BLOCK_SIZE, DEFAULT_SEQUENCE_BLOCK_SIZE);
        builder.threadPoolMaxSize = configurationSource.getInt(THREAD_POOL_MAX_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE);
    }
}
