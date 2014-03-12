package com.splicemachine.hbase.writer;

import java.util.concurrent.ExecutionException;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
class CountingWriteConfiguration extends ForwardingWriteConfiguration {
    private final BulkWriteAction.ActionStatusReporter statusReporter;

    public CountingWriteConfiguration(Writer.WriteConfiguration writeConfiguration, BulkWriteAction.ActionStatusReporter statusMonitor) {
				super(writeConfiguration);
        this.statusReporter = statusMonitor;
    }

    @Override
    public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
        statusReporter.globalFailures.incrementAndGet();
        if(t instanceof RpcClient.CallTimeoutException)
            statusReporter.timedOutFlushes.incrementAndGet();
        else if(t instanceof NotServingRegionException)
            statusReporter.notServingRegionFlushes.incrementAndGet();
        else if(t instanceof WrongRegionException)
            statusReporter.wrongRegionFlushes.incrementAndGet();
        return super.globalError(t);
    }

    @Override
    public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        statusReporter.partialFailures.incrementAndGet();
        //look for timeouts, not serving regions, wrong regions, and so forth
        boolean notServingRegion= false;
        boolean wrongRegion = false;
        boolean failed = false;
        boolean writeConflict = false;
        for(IntObjectCursor<WriteResult> cursor:result.getFailedRows()){
            WriteResult.Code code = cursor.value.getCode();
            switch (code) {
                case FAILED:
                    failed=true;
                    break;
                case WRITE_CONFLICT:
                    writeConflict=true;
                    break;
                case NOT_SERVING_REGION:
                    notServingRegion = true;
                    break;
                case WRONG_REGION:
                    wrongRegion = true;
                    break;
            }
        }
        if(notServingRegion)
            statusReporter.notServingRegionFlushes.incrementAndGet();
        if(wrongRegion)
            statusReporter.wrongRegionFlushes.incrementAndGet();
        if(writeConflict)
            statusReporter.writeConflictBufferFlushes.incrementAndGet();
        if(failed)
            statusReporter.failedBufferFlushes.incrementAndGet();
        return super.partialFailure(result,request);
    }
}
