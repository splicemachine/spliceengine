package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.RegionCache;

import com.splicemachine.stats.IOStats;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public abstract class BucketingWriter implements Writer{
    protected final RegionCache regionCache;
    protected final HConnection connection;

    protected BucketingWriter(RegionCache regionCache, HConnection connection) {
        this.regionCache = regionCache;
        this.connection = connection;
    }

//    @Override
    public final Future<WriteStats> write(byte[] tableName, ObjectArrayList<KVPair> buffer, String transactionId,WriteConfiguration writeConfiguration) throws ExecutionException {
        try {
            List<Throwable> errors = Lists.newArrayListWithExpectedSize(0);
            List<BulkWrite> bulkWrites = bucketWrites(writeConfiguration.getMaximumRetries(),tableName,buffer,transactionId,errors, writeConfiguration);
            CompositeFuture<WriteStats> compositeFuture = new CompositeFuture<WriteStats>();
            for(BulkWrite bulkWrite:bulkWrites){
                errors.clear();
                compositeFuture.add(write(tableName, bulkWrite, writeConfiguration));
            }
            return compositeFuture;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }


    protected final List<BulkWrite> bucketWrites(int tries,byte[] tableName,ObjectArrayList<KVPair> buffer,String txnId,List<Throwable> errors,WriteConfiguration writeConfiguration) throws Exception{
        if(tries<=0)
            throw getError(errors);

        Set<HRegionInfo> regions = regionCache.getRegions(tableName);
        if(regions.size()<=0){
            //TODO -sf- add error handling in here
            Thread.sleep(WriteUtils.getWaitTime(writeConfiguration.getMaximumRetries() - tries + 1, writeConfiguration.getPause()));
            regionCache.invalidate(tableName);
            errors.add(new IOException("Unable to determine regions for table "+ Bytes.toString(tableName)));
            return bucketWrites(tries-1,tableName,buffer,txnId,errors, writeConfiguration);
        }
        List<BulkWrite> buckets = Lists.newArrayListWithCapacity(regions.size());
        for(HRegionInfo info:regions){
            buckets.add(new BulkWrite(txnId,info.getStartKey()));
        }

        if(WriteUtils.bucketWrites(buffer,buckets)){
            return buckets;
        }else{
            //there were regions missing because they were splitting or something similar
            Thread.sleep(WriteUtils.getWaitTime(writeConfiguration.getMaximumRetries()-tries+1, writeConfiguration.getPause()));
            regionCache.invalidate(tableName);
            return bucketWrites(tries-1,tableName,buffer,txnId,errors, writeConfiguration);
        }
    }

    private Exception getError(List<Throwable> errors) {
        return new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
    }

    private static class CompositeFuture<T> implements Future<T>{
        private final List<Future<T>> futures = Lists.newArrayList();
        private volatile boolean cancelled = false;

        public void add(Future<T> future){
            this.futures.add(future);
        }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if(cancelled)
                 return true;

            for(Future<T> future:futures){
                cancelled = cancelled && future.cancel(mayInterruptIfRunning);
            }
            return cancelled;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            for(Future<T> future:futures){
                if(!future.isDone()) return false;
            }
            return true;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            //return the last entry
            T next = null;
            for(Future<T> future:futures){
                next = future.get();
            }
            return next;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            //return the last entry
            T next = null;
            long timeRemaining =unit.toNanos(timeout);
            for(Future<T> future:futures){
                if(timeRemaining<=0) return next;
                long start = System.nanoTime();
                next = future.get(timeout,unit);
                timeRemaining -=System.nanoTime()-start;
            }
            return next;
        }
    }

}
