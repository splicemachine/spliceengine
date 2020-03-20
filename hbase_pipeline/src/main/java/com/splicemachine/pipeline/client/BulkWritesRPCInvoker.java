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

package com.splicemachine.pipeline.client;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.ipc.RpcChannelFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWritesRPCInvoker implements BulkWriter{
    public static volatile boolean forceRemote = false;

    private final BulkWriteChannelInvoker bulkWriteChannelInvoker;
    private final WritePipelineFactory pipelineFactory;
    private final PipelineWriter pipelineWriter;

    public BulkWritesRPCInvoker(byte[] tableName,
                                PipelineWriter pipelineWriter,
                                WritePipelineFactory pipelineFactory,
                                PipelineCompressor pipelineCompressor,
                                PipelineExceptionFactory exceptionFactory,
                                RpcChannelFactory channelFactory,
                                PartitionInfoCache partInfoCache,
                                HBaseTableInfoFactory tableInfoFactory) {
        this.pipelineFactory = pipelineFactory;
        this.pipelineWriter = pipelineWriter;
        this.bulkWriteChannelInvoker = new BulkWriteChannelInvoker(tableName,pipelineCompressor,channelFactory,partInfoCache,exceptionFactory,tableInfoFactory);
    }

    @Override
    public BulkWritesResult write(final BulkWrites writes, boolean refreshCache) throws IOException {
        assert writes.numEntries() != 0;
        if(!forceRemote) {

            assert !writes.getBulkWrites().isEmpty(): "Invoked a write with no BulkWrite entities!";

            BulkWrite firstBulkWrite = writes.getBulkWrites().iterator().next();
            String encodedRegionName = firstBulkWrite.getEncodedStringName();
            PartitionWritePipeline pipeline=pipelineFactory.getPipeline(encodedRegionName);
            if(pipeline!=null){
                return pipelineWriter.bulkWrite(writes, -1);
            }
        }

        return bulkWriteChannelInvoker.invoke(writes);
    }
}
