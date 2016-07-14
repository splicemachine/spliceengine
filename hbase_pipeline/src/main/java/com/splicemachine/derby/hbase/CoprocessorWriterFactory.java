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

package com.splicemachine.derby.hbase;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.pipeline.client.BulkWritesRPCInvoker;
import com.splicemachine.pipeline.client.RpcChannelFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class CoprocessorWriterFactory implements BulkWriterFactory{
    private final PipelineExceptionFactory exceptionFactory;
    private PipelineWriter pipelineWriter;
    private WritePipelineFactory pipelineWriteFactory;
    private final PipelineCompressor compressor;
    private final PartitionInfoCache partitionInfoCache;
    private final RpcChannelFactory channelFactory;
    private final HBaseTableInfoFactory tableInfoFactory;

    public CoprocessorWriterFactory(PipelineCompressor compressor,
                                    PartitionInfoCache partitionInfoCache,
                                    PipelineExceptionFactory exceptionFactory,
                                    RpcChannelFactory channelFactory,
                                    HBaseTableInfoFactory tableInfoFactory){
        this.exceptionFactory=exceptionFactory;
        this.compressor = compressor;
        this.partitionInfoCache = partitionInfoCache;
        this.channelFactory=channelFactory;
        this.tableInfoFactory = tableInfoFactory;
    }

    @Override
    public BulkWriter newWriter(byte[] tableName){
        return new BulkWritesRPCInvoker(tableName,pipelineWriter,
                pipelineWriteFactory,compressor,
                exceptionFactory,channelFactory,partitionInfoCache,
                tableInfoFactory);
    }

    @Override
    public void invalidateCache(byte[] tableName) throws IOException{
        partitionInfoCache.invalidate(tableName);
    }

    @Override
    public void setPipeline(WritePipelineFactory writePipelineFactory){
       this.pipelineWriteFactory = writePipelineFactory;
    }

    @Override
    public void setWriter(PipelineWriter pipelineWriter){
        this.pipelineWriter = pipelineWriter;
    }
}
