package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.hbase.async.AsyncScannerUtils;
import com.splicemachine.hbase.async.SimpleAsyncScanner;
import com.splicemachine.metrics.*;
import com.splicemachine.metrics.Timer;
import com.stumbleupon.async.Callback;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import java.io.IOException;
import java.util.*;

/**
 * Asynchronous distributed scanner.
 *
 * This implementation splits a single scan into multiple independent scans, each of
 * which independently and asynchronously scans data for its respective region, feeding
 * it into a cache of results.
 *
 * This implementation does <em>not</em> guarantee that the results
 * returned will be in sorted order. However,  it will return <em>partially</em> sorted
 * results. That is, if there are 16 sub-scanners, then data can be out of order globally, but within
 * each scanner we are sorted.
 *
 * @author Scott Fines
 * Date: 7/14/14
 */
public class AsyncDistributedScanner implements SpliceResultScanner{
    private static final Logger LOG = Logger.getLogger(AsyncDistributedScanner.class);
    //used to indicate that the scanner has been exhausted
    private final Timer remoteTimer;
    private final Counter remoteBytesCounter;

    private volatile Throwable error;
    private final Callback<Object, Object> errorHandler = new Callback<Object, Object>() {
        @Override
        public Object call(Object arg) throws Exception {
            error = (Throwable)arg; //TODO -sf- this is just a guess
            return null;
        }
    };

    private final SimpleAsyncScanner[] scanners;
    private boolean [] activeScanners;


    private int readPosition = -1;
    private final int batchSize;

    public AsyncDistributedScanner(HBaseClient hbaseClient,
                                   byte[] tableName,
                                   Scan[] scans,
                                   MetricFactory factory){
        this(hbaseClient, tableName, scans, factory,128);
    }

    public AsyncDistributedScanner(HBaseClient hbaseClient,
                                   byte[] tableName,
                                   Scan[] scans,
                                   MetricFactory factory,
                                   int batchSize) {
        this.batchSize = batchSize;
        this.remoteBytesCounter = factory.newCounter();
        this.remoteTimer = factory.newTimer();

        this.scanners = new SimpleAsyncScanner[scans.length];
        this.activeScanners = new boolean[scans.length];
        Arrays.fill(activeScanners, true);
        for(int i=0;i<scanners.length;i++){
            //don't populate the block cache for temp results
            scanners[i] = new SimpleAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scans[i],tableName,hbaseClient,false),factory);
        }
    }

    public static AsyncDistributedScanner create(byte[] tableName, Scan[] scans, MetricFactory factory){
        //TODO -sf- replace HBASE_CLIENT with a better singleton
        return new AsyncDistributedScanner(SimpleAsyncScanner.HBASE_CLIENT,tableName,scans,factory);
    }

    public int subScannerCount(){ return scanners.length; }

    @Override
    public void open() throws IOException, StandardException {
        for(SimpleAsyncScanner scanner:scanners){
            scanner.open();
        }
    }

    @Override
    public TimeView getRemoteReadTime() {
        return remoteTimer.getTime();
    }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return remoteTimer.getNumEvents(); }

    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0; }
    @Override public long getLocalRowsRead() { return 0; }

    @Override
    public Result next() throws IOException {
        List<org.hbase.async.KeyValue> kvs = nextKvs();
        if(kvs!=null)
            return new Result(AsyncScannerUtils.convertFromAsync(kvs));
        return null;
    }

    @Override
    public Result[] next(int size) throws IOException {
        List<Result> results = Lists.newArrayListWithCapacity(size);
        for(int i=0;i<size;i++){
            results.add(next());
        }
        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close() {
        for(SimpleAsyncScanner scanner:scanners){
            scanner.close();
        }
    }

    /*
     * Direct access method to avoid the cost of converting between an async KeyValue object
     * and an hbase one
     */
    public List<org.hbase.async.KeyValue> nextKvs() throws IOException {
        if(error!=null)
            throw new IOException(error);

        remoteTimer.startTiming();
        //noinspection ForLoopReplaceableByForEach
        for(int i=0;i<scanners.length;i++){
            readPosition = (readPosition+1)%scanners.length;
            if(!activeScanners[readPosition]) continue; //skip known exhausted scanners
            try {
                List<org.hbase.async.KeyValue> r = scanners[readPosition].nextKeyValues();
                if(r==null){
                    activeScanners[readPosition] = false;
                }else {
                    remoteTimer.tick(1);
                    if(remoteBytesCounter.isActive()){
                        countKeyValues(r);
                    }
                    return r;
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        remoteTimer.stopTiming();
        return null; //we are empty
    }


    @Override
    public Iterator<Result> iterator() {
        return null;
    }

    /******************************************************************************************************************/
    /*private helper methods*/
    private void countKeyValues(List<KeyValue> r) {
        for(KeyValue kv:r){
            int size = kv.key().length + kv.family().length + kv.qualifier().length + kv.value().length + 8;
            remoteBytesCounter.add(size);
        }
    }

    
}
