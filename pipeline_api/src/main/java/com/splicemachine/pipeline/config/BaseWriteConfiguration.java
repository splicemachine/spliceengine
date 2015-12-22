package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;

public abstract class BaseWriteConfiguration implements WriteConfiguration {
    private static final Logger LOG = Logger.getLogger(BaseWriteConfiguration.class);

    protected final PipelineExceptionFactory exceptionFactory;

    public BaseWriteConfiguration(PipelineExceptionFactory exceptionFactory){
        this.exceptionFactory=exceptionFactory;
    }

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
        t=exceptionFactory.processPipelineException(t);
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return WriteResponse.IGNORE;
        } else if(exceptionFactory.needsTransactionalRetry(t)){
            return WriteResponse.THROW_ERROR;
        }else if(exceptionFactory.canFinitelyRetry(t)||exceptionFactory.canInfinitelyRetry(t))
            return WriteResponse.RETRY;
        else
            return WriteResponse.THROW_ERROR;
    }

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
        WriteResult writeResult = bulkWriteResult.getGlobalResult();
        if (writeResult.isSuccess())
            return WriteResponse.SUCCESS;
        else if (writeResult.isPartial()) {
            IntObjectOpenHashMap<WriteResult> failedRows = bulkWriteResult.getFailedRows();
            if (failedRows != null && failedRows.size() > 0) {
                return WriteResponse.PARTIAL;
            }
            IntOpenHashSet notRun = bulkWriteResult.getNotRunRows();
            if(notRun!=null && notRun.size()>0)
                return WriteResponse.PARTIAL;
            /*
             * We got a partial result, but didn't specify which rows needed behavior.
             * That's weird, but since we weren't told there would be a problem, we may
             * as well ignore
             */
            return WriteResponse.IGNORE;
        } else if (!writeResult.canRetry())
            throw exceptionFactory.processErrorResult(writeResult);
        else
            return WriteResponse.RETRY;
    }

    @Override
    public void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
        SpliceLogUtils.warn(LOG, "registering Context with a base class");
    }

    @Override
    public PipelineExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

}
