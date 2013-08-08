package com.splicemachine.hbase.writer;

import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.RegionCache;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * eated on: 8/8/13
 */
public class AsyncWriter extends AbstractWriter {
    private final MonitoredThreadPool writerPool;

    public AsyncWriter(MonitoredThreadPool writerPool,
                          RegionCache regionCache,
                          HConnection connection) {
        super(regionCache, connection);
        this.writerPool = writerPool;
    }

    @Override
    protected Future<Void> write(byte[] tableName, BulkWrite bulkWrite,RetryStrategy retryStrategy) throws ExecutionException {
        BulkWriteAction action = new BulkWriteAction(tableName,
                bulkWrite,
                regionCache,
                retryStrategy,
                connection);
        return writerPool.submit(action);
    }


    @Override
    public void stopWrites() {
        writerPool.shutdown();
    }
}
