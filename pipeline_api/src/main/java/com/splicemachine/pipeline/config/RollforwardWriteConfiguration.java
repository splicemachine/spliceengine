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

package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.access.api.CallTimeoutException;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.ActionStatusReporter;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class RollforwardWriteConfiguration extends ForwardingWriteConfiguration{

    public RollforwardWriteConfiguration(WriteConfiguration writeConfiguration){
        super(writeConfiguration);
    }


    @Override
    public String toString(){
        return String.format("RollforwardConfiguration{delegate=%s}",this.delegate);
    }

    @Override
    public boolean rollForward() {
        return true;
    }
}
