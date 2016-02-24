package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.client.*;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.traffic.AtomicSpliceWriteControl;
import com.splicemachine.pipeline.traffic.SpliceWriteControl;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.utils.Pair;

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
    private final SpliceWriteControl writeControl;
    private final AtomicLong rejectedCount = new AtomicLong(0l);

    private volatile WriteCoordinator writeCoordinator;
    private final PipelineExceptionFactory exceptionFactory;
    private final WritePipelineFactory writePipelineFactory;

    public PipelineWriter(PipelineExceptionFactory pipelineExceptionFactory,
                          WritePipelineFactory writePipelineFactory,
                          SpliceWriteControl writeControl){
        this.writeControl = writeControl;
        this.exceptionFactory = pipelineExceptionFactory;
        this.writePipelineFactory = writePipelineFactory;

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
                rejectAll(bws,result,Code.NOT_SERVING_REGION);
                return new BulkWritesResult(result);
            }
            dependent = pwp.isDependent(bulkWrites.getTxn());
        } catch (InterruptedException e1) {
            throw new IOException(e1);
        } catch (IndexNotSetUpException e1) {
            rejectAll(bws,result,Code.INDEX_NOT_SETUP_EXCEPTION);
            return new BulkWritesResult(result);
        }

        AtomicSpliceWriteControl.Status status;
        int numKVPairs = bulkWrites.numEntries();  // KVPairs are just Splice mutations.  You can think of this count as rows modified (written to).
        // Get the "permit" to write.  WriteControl does not perform the writes.  It just controls whether or not the write is allowed to proceed.

        status = (dependent) ? writeControl.performDependentWrite(numKVPairs) : writeControl.performIndependentWrite(numKVPairs);
        if (status.equals(AtomicSpliceWriteControl.Status.REJECTED)) {
            rejectAll(bws,result, Code.PIPELINE_TOO_BUSY);
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
                pair.setFirst(finishResult);
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void rejectAll(Collection<BulkWrite> writes, Collection<BulkWriteResult> result, Code status) {
//        this.meter; //TODO -sf- add this back in
        for(BulkWrite write:writes){
            switch (status) {
                case PIPELINE_TOO_BUSY:
                    result.add(new BulkWriteResult(WriteResult.pipelineTooBusy(write.getEncodedStringName())));
                    break;
                case INDEX_NOT_SETUP_EXCEPTION:
                    result.add(new BulkWriteResult(WriteResult.indexNotSetup()));
                    break;
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
                writeResult = new BulkWriteResult(WriteResult.notServingRegion());
            }
            writePairMap.put(bw, Pair.newPair(writeResult, writePipeline));
        }
        return writePairMap;
    }


}
