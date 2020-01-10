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
