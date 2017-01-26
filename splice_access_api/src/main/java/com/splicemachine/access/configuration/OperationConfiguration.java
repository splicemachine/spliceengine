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
