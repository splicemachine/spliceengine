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

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.client.*;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.traffic.SpliceWriteControl;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
@ThreadSafe
public class PipelineWriter{
    private static final Logger LOG =Logger.getLogger(PipelineWriter.class);
    private final SpliceWriteControl writeControl;
    private final AtomicLong rejectedCount = new AtomicLong(0l);

    private volatile WriteCoordinator writeCoordinator;
    private final PipelineExceptionFactory exceptionFactory;
    private final WritePipelineFactory writePipelineFactory;
    private final PipelineMeter pipelineMeter;

    public PipelineWriter(PipelineExceptionFactory pipelineExceptionFactory,
                          WritePipelineFactory writePipelineFactory,
                          SpliceWriteControl writeControl,
                          PipelineMeter pipelineMeter){
        this.writeControl = writeControl;
        this.exceptionFactory = pipelineExceptionFactory;
        this.writePipelineFactory = writePipelineFactory;
        this.pipelineMeter = pipelineMeter;
    }


    public BulkWritesResult bulkWrite(@Nonnull BulkWrites bulkWrites) throws IOException{
        Collection<BulkWrite> bws = bulkWrites.getBulkWrites();
        int numBulkWrites = bulkWrites.getBulkWrites().size();
        List<BulkWriteResult> result = new ArrayList<>(numBulkWrites);
        SharedCallBufferFactory indexWriteBufferFactory = new SharedCallBufferFactory(writeCoordinator);

        if (numBulkWrites==0) {
            throw exceptionFactory.doNotRetry("Should Never Send Empty Call to Endpoint");
        }

        // Determine whether or not this write is dependent or independent.  Dependent writes are writes to a table with indexes.
        boolean dependent;
        try {
            Iterator<BulkWrite> iterator=bws.iterator();
            PartitionWritePipeline pwp = null;
            String eN= null;
            while(iterator.hasNext()){
                BulkWrite bw=iterator.next();
                String encodedName = bw.getEncodedStringName();
                if(encodedName==null) continue;
                if(eN==null)
                    eN = encodedName;

                pwp=writePipelineFactory.getPipeline(encodedName);
                if(pwp!=null){
                    break;
                }
            }
            if(pwp==null){
                if(LOG.isTraceEnabled())
                    LOG.trace("Rejecting "+bulkWrites.numEntries()+" rows in "+ bws.size()+"writes because we aren't serving any of those regions");
                rejectAll(bws,result,Code.NOT_SERVING_REGION,"No WritePipeline found in registry for BulkWrites "+ bulkWrites);
                return new BulkWritesResult(result);
            }
            dependent = pwp.isDependent(bulkWrites.getTxn());
        } catch (InterruptedException e1) {
            throw new IOException(e1);
        } catch (IndexNotSetUpException e1) {
            rejectAll(bws,result,Code.INDEX_NOT_SETUP_EXCEPTION,null);
            return new BulkWritesResult(result);
        }

        SpliceWriteControl.Status status;
        int numKVPairs = bulkWrites.numEntries();  // KVPairs are just Splice mutations.  You can think of this count as rows modified (written to).
        // Get the "permit" to write.  WriteControl does not perform the writes.  It just controls whether or not the write is allowed to proceed.

        status = (dependent) ? writeControl.performDependentWrite(numKVPairs) : writeControl.performIndependentWrite(numKVPairs);
        if (status.equals(SpliceWriteControl.Status.REJECTED)) {
            if(LOG.isTraceEnabled())
                LOG.trace("Rejecting "+numBulkWrites+" rows in "+ bws.size()+"writes because the pipeline is too busy");
            rejectAll(bws,result, Code.PIPELINE_TOO_BUSY,null);
            rejectedCount.addAndGet(numBulkWrites);
            return new BulkWritesResult(result);
        }
        try {
            return performWrite(bulkWrites,bws,result,indexWriteBufferFactory);
        } finally {
            switch (status) {
                case REJECTED:
                    break;
                case DEPENDENT:
                    writeControl.finishDependentWrite(numKVPairs);
                    break;
                case INDEPENDENT:
                    writeControl.finishIndependentWrite(numKVPairs);
                    break;
            }
        }
    }

    protected BulkWritesResult performWrite(@Nonnull BulkWrites bulkWrites,Collection<BulkWrite> bws,List<BulkWriteResult> result,SharedCallBufferFactory indexWriteBufferFactory) throws IOException{
        // Add the writes to the writePairMap, which helps link the BulkWrites to their result and write pipeline objects.
        Map<BulkWrite, Pair<BulkWriteResult, PartitionWritePipeline>> writePairMap = getBulkWritePairMap(bws);

        //
        // Submit the bulk writes for which we found a PartitionWritePipeline.
        //
        for (Map.Entry<BulkWrite, Pair<BulkWriteResult, PartitionWritePipeline>> entry : writePairMap.entrySet()) {
            Pair<BulkWriteResult, PartitionWritePipeline> pair = entry.getValue();
            PartitionWritePipeline writePipeline = pair.getSecond();
            if (writePipeline != null) {
                BulkWrite bulkWrite = entry.getKey();
                BulkWriteResult submitResult = writePipeline.submitBulkWrite(bulkWrites.getTxn(), bulkWrite,indexWriteBufferFactory, writePipeline.getRegionCoprocessorEnvironment());
                if(LOG.isTraceEnabled()){
                    LOG.trace("Submission of "+bulkWrite.getSize()+" rows to region "+ bulkWrite.getEncodedStringName()+" has submission result "+ submitResult.getGlobalResult());
                    if(submitResult.getFailedRows().size()>0){
                        LOG.trace("Detected "+ submitResult.getFailedRows().size()+" failed rows");
                    }
                    if(submitResult.getNotRunRows().size()>0){
                        LOG.trace("Detected "+ submitResult.getNotRunRows().size()+" not run rows");
                    }
                }
                pair.setFirst(submitResult);
            }
        }

        //
        // Same iteration, now calling finishWrite() for each BulkWrite
        //
        for (Map.Entry<BulkWrite, Pair<BulkWriteResult, PartitionWritePipeline>> entry : writePairMap.entrySet()) {
            Pair<BulkWriteResult, PartitionWritePipeline> pair = entry.getValue();
            PartitionWritePipeline writePipeline = pair.getSecond();
            if (writePipeline != null) {
                BulkWrite bulkWrite = entry.getKey();
                BulkWriteResult writeResult = pair.getFirst();
                BulkWriteResult finishResult = writePipeline.finishWrite(writeResult, bulkWrite);
                if(LOG.isTraceEnabled()){
                    LOG.trace("Finish of "+bulkWrite.getSize()+" rows to region "+ bulkWrite.getEncodedStringName()+" has finish result "+ finishResult.getGlobalResult());
                    if(finishResult.getFailedRows().size()>0){
                        LOG.trace("Detected "+ finishResult.getFailedRows().size()+" failed rows");
                    }
                    if(finishResult.getNotRunRows().size()>0){
                        LOG.trace("Detected "+ finishResult.getNotRunRows().size()+" not run rows");
                    }
                }
                pair.setFirst(finishResult);
                pipelineMeter.mark(bulkWrite.getSize()-finishResult.getFailedRows().size(),finishResult.getFailedRows().size());
            }
        }

            /*
             * Collect the overall results.
             *
             * It is IMPERATIVE that we collect results in the *same iteration order*
             * as we received the writes, otherwise we won't be interpreting the correct
             * results on the other side; the end result will be extraneous errors, but only at scale,
             * so you won't necessarily see the errors in the ITs and you'll think everything is fine,
             * but it's not. I assure you.
             */
        for(BulkWrite bw:bws){
            Pair<BulkWriteResult,PartitionWritePipeline> results = writePairMap.get(bw);
            result.add(results.getFirst());
        }
        return new BulkWritesResult(result);
    }

    public void setWriteCoordinator(WriteCoordinator writeCoordinator){
        this.writeCoordinator = writeCoordinator;
    }

    public WriteCoordinator getWriteCoordinator(){
        return writeCoordinator;
    }

    public BulkWritesResult rejectAll(BulkWrites bulkWrites,WriteResult globalStatus) {
        Collection<BulkWriteResult> results = new ArrayList<>(bulkWrites.numRegions());
        for(int i=0;i<bulkWrites.numRegions();i++){
            BulkWriteResult result=new BulkWriteResult();
            result.setGlobalStatus(globalStatus);
            results.add(result);
        }
        return new BulkWritesResult(results);
    }
    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void rejectAll(Collection<BulkWrite> writes, Collection<BulkWriteResult> result, Code status,String msg) {
        for(BulkWrite write:writes){
            pipelineMeter.mark(0,write.getSize());
            switch (status) {
                case NOT_SERVING_REGION:
                    result.add(new BulkWriteResult(WriteResult.notServingRegion(msg)));
                    break;
                case PIPELINE_TOO_BUSY:
                    result.add(new BulkWriteResult(WriteResult.pipelineTooBusy(write.getEncodedStringName())));
                    break;
                case INDEX_NOT_SETUP_EXCEPTION:
                    if(LOG.isTraceEnabled())
                        LOG.trace("Rejecting "+write.getSize()+" rows because region "+ write.getEncodedStringName()+" has not setup its write pipeline");
                    result.add(new BulkWriteResult(WriteResult.indexNotSetup()));
                    break;
                default:
                    //add in a default pattern here so that we don't accidentally ignore things
                    result.add(new BulkWriteResult(new WriteResult(status,msg)));
            }
        }
    }

    /**
     * Just builds this map:  BulkWrite -> (BulkWriteResult, PartitionWritePipeline) where the PartitionWritePipeline may
     * be null for some BulkWrites.
     */
    private Map<BulkWrite, Pair<BulkWriteResult, PartitionWritePipeline>> getBulkWritePairMap(Collection<BulkWrite> buffer) {
        Map<BulkWrite, Pair<BulkWriteResult, PartitionWritePipeline>> writePairMap = new IdentityHashMap<>();
        for(BulkWrite bw:buffer){
            PartitionWritePipeline writePipeline = writePipelineFactory.getPipeline(bw.getEncodedStringName());
            BulkWriteResult writeResult;
            if (writePipeline != null) {
                //we might be able to write this one
                writeResult = new BulkWriteResult();
            } else {
                if(LOG.isTraceEnabled())
                    LOG.trace("Rejecting "+bw.getSize()+" rows because region "+ bw.getEncodedStringName()+" is not being served");
                writeResult = new BulkWriteResult(WriteResult.notServingRegion());
                pipelineMeter.rejected(bw.getSize());
            }
            writePairMap.put(bw, Pair.newPair(writeResult, writePipeline));
        }
        return writePairMap;
    }


}
