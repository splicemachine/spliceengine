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
