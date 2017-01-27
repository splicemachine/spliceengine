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

package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.callbuffer.Rebuildable;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutionException;

public class UpdatingWriteConfiguration extends ForwardingWriteConfiguration{
    Rebuildable rebuildable;

    public UpdatingWriteConfiguration(WriteConfiguration delegate,
                                      Rebuildable rebuildable){
        super(delegate);
        this.rebuildable=rebuildable;
    }

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException{
        t=getExceptionFactory().processPipelineException(t);
        if(t instanceof NotServingPartitionException || t instanceof WrongPartitionException)
            rebuildable.rebuild();

        return super.globalError(t);
    }

    @Override
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    public WriteResponse partialFailure(BulkWriteResult result,BulkWrite request) throws ExecutionException{
        for(IntObjectCursor<WriteResult> cursor : result.getFailedRows()){
            switch(cursor.value.getCode()){
                case NOT_SERVING_REGION:
                case WRONG_REGION:
                    rebuildable.rebuild();
                    break;
            }
        }
        return super.partialFailure(result,request);
    }

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult)
            throws Throwable{
        if(bulkWriteResult.getGlobalResult().refreshCache())
            rebuildable.rebuild();
        return super.processGlobalResult(bulkWriteResult);
    }

    @Override
    public String toString(){
        return String.format("UpdatingWriteConfiguration{delegate=%s}",delegate);
    }


}

