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

package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.impl.driver.SIEnvironment;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PipelineEnvironment extends SIEnvironment{
    PipelineExceptionFactory pipelineExceptionFactory();

    PipelineDriver getPipelineDriver();

    ContextFactoryDriver contextFactoryDriver();

    PipelineCompressor pipelineCompressor();

    BulkWriterFactory writerFactory();

    PipelineMeter pipelineMeter();

    WritePipelineFactory pipelineFactory();
}
