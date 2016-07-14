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

package com.splicemachine.pipeline.mem;

import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.*;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.traffic.SpliceWriteControl;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
@ThreadSafe
public class DirectBulkWriterFactory implements BulkWriterFactory{
    private volatile PipelineWriter pipelineWriter;

    public DirectBulkWriterFactory(WritePipelineFactory wpf,
                                   SpliceWriteControl writeControl,
                                   PipelineExceptionFactory exceptionFactory,
                                   PipelineMeter meter) throws IOException{
        this.pipelineWriter = new PipelineWriter(exceptionFactory,wpf,writeControl,meter);
    }

    public void setWriteCoordinator(WriteCoordinator writeCoordinator){
        this.pipelineWriter.setWriteCoordinator(writeCoordinator);
    }

    @Override
    public BulkWriter newWriter(byte[] tableName){
        return new DirectBulkWriter(pipelineWriter);
    }

    @Override
    public void invalidateCache(byte[] tableName){
        //no-op for in-memory
    }

    @Override
    public void setPipeline(WritePipelineFactory writePipelineFactory){
        //no-op
    }

    @Override
    public void setWriter(PipelineWriter pipelineWriter){
        this.pipelineWriter = pipelineWriter;
    }
}
