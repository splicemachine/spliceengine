package com.splicemachine.pipeline.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.splicemachine.pipeline.impl.BulkWrites;
/**
 * Handler for rejected writes from the Monitored Thread Pool.  These writes are not rejected from the server(s) that you are sending them 
 * to rather rejected from the thread pool.
 * 
 */
public interface WriteRejectedHandler{
    public Future<WriteStats> writeRejected(byte[] tableName, BulkWrites action, WriteConfiguration writeConfiguration) throws ExecutionException;
}

