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

package com.splicemachine.pipeline;

import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.RegionBusyException;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.PipelineTooBusy;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * The entry/starting point for BulkWrites remotely (on the region server for the table they will mutate).
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class PartitionWritePipeline{

    private static final BulkWriteResult NOT_SERVING_REGION=new BulkWriteResult(null,WriteResult.notServingRegion());
    private static final BulkWriteResult INTERRUPTED=new BulkWriteResult(null,WriteResult.interrupted());
    private static final BulkWriteResult INDEX_NOT_SETUP=new BulkWriteResult(null,WriteResult.indexNotSetup());

    private final WriteContextFactory<TransactionalRegion> ctxFactory;
    private final Partition region;
    private final TransactionalRegion txnRegion;
    private final PipelineMeter pipelineMeters;
    private final ServerControl rce;
    private final PipelineExceptionFactory exceptionFactory;

    public PartitionWritePipeline(ServerControl rce,
                                  Partition region,
                                  WriteContextFactory<TransactionalRegion> ctxFactory,
                                  TransactionalRegion txnRegion,
                                  PipelineMeter pipelineMeters,
                                  PipelineExceptionFactory exceptionFactory){
        this.rce=rce;
        this.region=region;
        this.ctxFactory=ctxFactory;
        this.txnRegion=txnRegion;
        this.pipelineMeters=pipelineMeters;
        this.exceptionFactory=exceptionFactory;
    }

    public ServerControl getRegionCoprocessorEnvironment(){
        return rce;
    }

    public void close(){
        ctxFactory.close();
        txnRegion.close();
    }


    public BulkWriteResult submitBulkWrite(TxnView txn,
                                           BulkWrite toWrite,
                                           SharedCallBufferFactory writeBufferFactory,
                                           ServerControl rce) throws IOException{
        assert txn!=null:"No transaction specified!";

        /*
         * We don't need to actually start a region operation here,
         * because we know that the actual writes won't happen
         * until we call finishWrite() below. We do a quick check
         * to make sure that the region isn't closing, but other
         * than that, we don't need to do anything regional here
         */
        if(region.isClosed() || region.isClosing()){
            return NOT_SERVING_REGION;
        }

        WriteContext context;
        try{
            context=ctxFactory.create(writeBufferFactory,txn,txnRegion,toWrite.getSize(),toWrite.skipIndexWrite(),toWrite.skipConflictDetection(),rce);
        }catch(InterruptedException e){
            return INTERRUPTED;
        }catch(IndexNotSetUpException e){
            return INDEX_NOT_SETUP;
        }
        Collection<KVPair> kvPairs=toWrite.getMutations();
        for(KVPair kvPair : kvPairs){
            context.sendUpstream(kvPair);
        }
        return new BulkWriteResult(context,WriteResult.success());
    }

    public BulkWriteResult finishWrite(BulkWriteResult intermediateResult,BulkWrite write) throws IOException{
        WriteContext ctx=intermediateResult.getWriteContext();
        if(ctx==null)
            return intermediateResult; //already failed

        try{
            region.startOperation();
        }catch(IOException nsre){
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable throwable=exceptionFactory.processPipelineException(nsre);
            if(throwable instanceof NotServingPartitionException)
                return new BulkWriteResult(WriteResult.notServingRegion());
            else if(throwable instanceof PipelineTooBusy)
                return new BulkWriteResult(WriteResult.regionTooBusy());
            else if(throwable instanceof RegionBusyException)
                return new BulkWriteResult(WriteResult.regionTooBusy());
            else if(throwable instanceof InterruptedException)
                return new BulkWriteResult(WriteResult.interrupted());
            else
                throw nsre;
        }
        try{
            ctx.flush();
            Map<KVPair, WriteResult> rowResultMap=ctx.close();
            BulkWriteResult response=new BulkWriteResult();
            int failed=0;
            int size=write.getSize();
            Collection<KVPair> kvPairs=write.getMutations();
            int i=0;
            for(KVPair kvPair : kvPairs){
                WriteResult result=rowResultMap.get(kvPair);
                if(result==null){
                    /* in case a kvPair is of CANCEL type, it may be ignored and no result is returned.
                     * Mark the result as SUCCESS
                     * */
                    result=new WriteResult(Code.SUCCESS);
                }
                if(!result.isSuccess())
                    failed++;
                response.addResult(i,result);
                i++;
            }
            if(failed>0){
                response.setGlobalStatus(WriteResult.partial());
            }else
                response.setGlobalStatus(WriteResult.success());

            pipelineMeters.mark(size-failed,failed);
            return response;
        }catch(IOException nsre){
            Throwable throwable=exceptionFactory.processPipelineException(nsre);
            if(throwable instanceof NotServingPartitionException)
                return new BulkWriteResult(WriteResult.notServingRegion());
            else if(throwable instanceof PipelineTooBusy)
                return new BulkWriteResult(WriteResult.regionTooBusy());
            else if(throwable instanceof RegionBusyException)
                return new BulkWriteResult(WriteResult.regionTooBusy());
            else if(throwable instanceof InterruptedException)
                return new BulkWriteResult(WriteResult.interrupted());
            else
                throw nsre;
        }finally{
            region.closeOperation();
        }
    }

    public boolean isDependent(TxnView txn) throws IOException, InterruptedException{
        return ctxFactory.hasDependentWrite(txn);
    }

    public WriteContextFactory<TransactionalRegion> getContextFactory(){
        return ctxFactory;
    }
}
