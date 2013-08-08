package com.splicemachine.hbase.writer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public interface Writer {

    public Future<Void> write(byte[] tableName,List<KVPair> buffer,String transactionId,RetryStrategy retryStrategy) throws ExecutionException;

    void stopWrites();

    public enum WriteResponse{
        THROW_ERROR,
        RETRY,
        IGNORE
    }

    public interface RetryStrategy{

        int getMaximumRetries();

        WriteResponse globalError(Throwable t) throws ExecutionException;

        WriteResponse partialFailure(BulkWriteResult result,BulkWrite request) throws ExecutionException;

        long getPause();
    }

}
