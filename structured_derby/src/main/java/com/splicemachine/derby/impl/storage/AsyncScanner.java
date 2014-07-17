package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.metrics.*;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.hbase.async.*;
import org.hbase.async.Scanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 7/14/14
 */
public class AsyncScanner implements SpliceResultScanner, Callback<ArrayList<ArrayList<KeyValue>>, ArrayList<ArrayList<KeyValue>>> {
    static final HBaseClient HBASE_CLIENT;
    static{
        String zkQuorumStr = SpliceConstants.config.get(HConstants.ZOOKEEPER_QUORUM);
        HBASE_CLIENT = new HBaseClient(zkQuorumStr);
    }

    private final Timer timer;
    private final Counter remoteBytesCounter;

    private final Scanner scanner;
    private final Queue<List<KeyValue>> resultQueue;
    private final int batchSize;

    private volatile Deferred<ArrayList<ArrayList<KeyValue>>> outstandingRequest;
    private volatile Deferred<ArrayList<ArrayList<KeyValue>>> finishedRequest;


    public AsyncScanner(byte[] table, Scan scan, MetricFactory metricFactory) {
       this(table,scan,metricFactory,128);
    }

    public AsyncScanner(byte[] table, Scan scan, MetricFactory metricFactory, int batchSize) {
       this(table,scan,metricFactory,batchSize,true);
    }

    public AsyncScanner(byte[] table, Scan scan, MetricFactory metricFactory, int batchSize,boolean populateBlockCache) {
        this.batchSize = batchSize;
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();

        //TODO -sf- use a different HBaseClient singleton pattern so it can be shutdown easily
        this.scanner = AsyncScannerUtils.convertScanner(scan,table,HBASE_CLIENT,populateBlockCache);
        this.resultQueue = new LinkedList<List<KeyValue>>();
    }

    @Override
    public void open() throws IOException, StandardException {
       //initiate the first scan
        outstandingRequest = scanner.nextRows().addCallback(this);
    }

    @Override public TimeView getRemoteReadTime() { return timer.getTime(); }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return timer.getNumEvents(); }

    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0; }
    @Override public long getLocalRowsRead() { return 0; }

    public List<org.hbase.async.KeyValue> nextKeyValues() throws Exception{
        List<KeyValue> row = resultQueue.poll();
        if(row!=null) return row;

        Deferred<ArrayList<ArrayList<KeyValue>>> deferred = finishedRequest;
        if(deferred==null)
            deferred = outstandingRequest;

        if(deferred==null) return null; //scanner is exhausted

        ArrayList<ArrayList<KeyValue>> kvs = deferred.join();
        finishedRequest=null;

        if(kvs==null||kvs.size()<=0) return null;
        if(kvs.size()>=batchSize){
            outstandingRequest = scanner.nextRows().addCallback(this);
        }
        List<KeyValue> first = kvs.get(0);
        for(int i=1;i<kvs.size();i++){
            resultQueue.offer(kvs.get(i));
        }
        return first;
    }

    @Override
    public Result next() throws IOException {
        try {
            List<KeyValue> kvs = nextKeyValues();
            if(kvs!=null && kvs.size()>0)
                return new Result(AsyncScannerUtils.convertFromAsync(kvs));
            return null;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Result[] next(int size) throws IOException {
        Result[] results = new Result[size];
        Result next;
        int i=0;
        while(i<size && (next = next())!=null){
            results[i] = next;
            i++;
        }
        return results;
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public Iterator<Result> iterator() {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public static void main(String...args) throws Exception {
        Logger.getRootLogger().addAppender(new ConsoleAppender(new SimpleLayout()));
        byte[] table = Bytes.toBytes(Long.toString(16));
        HBaseClient client = new HBaseClient("localhost","/hbase");
        client.ensureTableExists(table).join();
        System.out.println("Client constructed, opening scanner");
        Scanner scanner = client.newScanner(table);
        Deferred<ArrayList<ArrayList<KeyValue>>> arrayListDeferred = scanner.nextRows();
        System.out.println(arrayListDeferred);
        arrayListDeferred.addErrback(new Callback<Object, Object>() {
            @Override
            public Object call(Object arg) throws Exception {
                System.out.println("Error!"+arg);
                return null;
            }
        });
        ArrayList<ArrayList<KeyValue>> join = arrayListDeferred.join();
        System.out.println("Received rows");
        client.shutdown().join();
    }

    @Override
    public ArrayList<ArrayList<KeyValue>> call(ArrayList<ArrayList<KeyValue>> arg) throws Exception {
        finishedRequest = outstandingRequest;
        outstandingRequest = null;
        return arg;
    }
}
