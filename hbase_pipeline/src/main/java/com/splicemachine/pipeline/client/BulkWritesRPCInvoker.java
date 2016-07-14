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

package com.splicemachine.pipeline.client;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
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
                return pipelineWriter.bulkWrite(writes);
            }
        }

        return bulkWriteChannelInvoker.invoke(writes);
    }
}