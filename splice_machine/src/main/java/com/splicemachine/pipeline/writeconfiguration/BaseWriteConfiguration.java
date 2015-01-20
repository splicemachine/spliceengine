package com.splicemachine.pipeline.writeconfiguration;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.splicemachine.derby.hbase.ExceptionTranslator;
import org.apache.log4j.Logger;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.ExceptionTranslator;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;

public abstract class BaseWriteConfiguration implements WriteConfiguration {
    private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static final Logger LOG = Logger.getLogger(BaseWriteConfiguration.class);

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
        ExceptionTranslator handler = derbyFactory.getExceptionHandler();
        if (handler.isInterruptedException(t)) {
            Thread.currentThread().interrupt();
            return WriteResponse.IGNORE;
        } else if ((handler.canFinitelyRetry(t) || handler.canInfinitelyRetry(t)) && !handler.needsTransactionalRetry(t))
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
            throw Exceptions.fromString(writeResult);
        else
            return WriteResponse.RETRY;
    }

    @Override
    public void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
        SpliceLogUtils.warn(LOG, "registering Context with a base class");
    }

}
