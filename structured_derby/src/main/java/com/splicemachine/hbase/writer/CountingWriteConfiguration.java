package com.splicemachine.hbase.writer;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
class CountingWriteConfiguration implements Writer.WriteConfiguration {
    private final Writer.WriteConfiguration delegate;
    private final BulkWriteAction.ActionStatusReporter statusReporter;

    public CountingWriteConfiguration(Writer.WriteConfiguration writeConfiguration, BulkWriteAction.ActionStatusReporter statusMonitor) {
        this.delegate = writeConfiguration;
        this.statusReporter = statusMonitor;
    }

    @Override
    public int getMaximumRetries() {
        return delegate.getMaximumRetries();
    }

    @Override
    public Writer.WriteResponse globalError(Throwable t) throws ExecutionException {
        statusReporter.globalFailures.incrementAndGet();
        if(t instanceof HBaseClient.CallTimeoutException)
            statusReporter.timedOutFlushes.incrementAndGet();
        else if(t instanceof NotServingRegionException)
            statusReporter.notServingRegionFlushes.incrementAndGet();
        else if(t instanceof WrongRegionException)
            statusReporter.wrongRegionFlushes.incrementAndGet();
        return delegate.globalError(t);
    }

    @Override
    public Writer.WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        statusReporter.partialFailures.incrementAndGet();
        //look for timeouts, not serving regions, wrong regions, and so forth
        boolean notServingRegion= false;
        boolean wrongRegion = false;
        boolean failed = false;
        boolean writeConflict = false;
        for(WriteResult writeResult:result.getFailedRows().values){
						if(writeResult==null)continue;
            WriteResult.Code code = writeResult.getCode();
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
        return delegate.partialFailure(result,request);
    }

    @Override
    public long getPause() {
        return delegate.getPause();
    }

    @Override
    public void writeComplete(long timeTakenMs, long numRecordsWritten) {
        delegate.writeComplete(timeTakenMs,numRecordsWritten);
    }
}
