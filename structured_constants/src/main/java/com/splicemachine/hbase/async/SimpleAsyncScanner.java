package com.splicemachine.hbase.async;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.stats.*;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.NullStopIterator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 7/14/14
 */
public class SimpleAsyncScanner implements AsyncScanner,Callback<ArrayList<ArrayList<KeyValue>>, ArrayList<ArrayList<KeyValue>>> {
    public static final HBaseClient HBASE_CLIENT;
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


    public SimpleAsyncScanner(Scanner scanner,
                              MetricFactory metricFactory){
        this.batchSize = scanner.getMaxNumRows();
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();

        this.scanner = scanner;
        this.resultQueue = new LinkedList<List<KeyValue>>();
    }

    @Override
    public void open() throws IOException {
       //initiate the first scan
        outstandingRequest = scanner.nextRows().addCallback(this);
    }

    @Override public TimeView getRemoteReadTime() { return timer.getTime(); }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return timer.getNumEvents(); }

    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0; }
    @Override public long getLocalRowsRead() { return 0; }

    @Override
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
        //issue the next request
        if(!scanner.onFinalRegion() || kvs.size()>=batchSize){
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
        return new NullStopIterator<Result>() {
            @Override public void close() throws IOException { SimpleAsyncScanner.this.close();  }
            @Override protected Result nextItem() throws IOException { return SimpleAsyncScanner.this.next(); }
        };
    }

    @Override
    public ArrayList<ArrayList<KeyValue>> call(ArrayList<ArrayList<KeyValue>> arg) throws Exception {
        finishedRequest = outstandingRequest;
        outstandingRequest = null;
        return arg;
    }
}
