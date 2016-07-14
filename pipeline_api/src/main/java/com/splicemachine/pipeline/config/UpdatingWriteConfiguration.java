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

