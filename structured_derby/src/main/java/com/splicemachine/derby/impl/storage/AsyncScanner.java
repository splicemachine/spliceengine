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
import org.hbase.async.ClientStats;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 7/14/14
 */
public class AsyncScanner implements SpliceResultScanner {
    static final HBaseClient HBASE_CLIENT;
    static{
        String zkQuorumStr = SpliceConstants.config.get(HConstants.ZOOKEEPER_QUORUM);
        HBASE_CLIENT = new HBaseClient(zkQuorumStr);
    }
    private final Scanner scanner;
    private final Timer timer;
    private final Counter remoteBytesCounter;

    private final List<List<KeyValue>> cache = Lists.newArrayList();

    public AsyncScanner(byte[] table,Scan scan,MetricFactory metricFactory) {
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();

        this.scanner = HBASE_CLIENT.newScanner(table);
        scanner.setStartKey(scan.getStartRow());
        scanner.setStopKey(scan.getStopRow());
        //TODO -sf- set scan filters
//        scanner.setFilter(scan.getFilter());
    }

    @Override public void open() throws IOException, StandardException {  }

    @Override public TimeView getRemoteReadTime() { return timer.getTime(); }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return timer.getNumEvents(); }

    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0; }
    @Override public long getLocalRowsRead() { return 0; }

    @Override
    public Result next() throws IOException {
        if(cache.size()>0){
            return newResult(cache.remove(0));
        }
        Deferred<ArrayList<ArrayList<KeyValue>>> arrayListDeferred = scanner.nextRows();
        if(arrayListDeferred==null){
            scanner.close();
            return null;
        }
        try {
            ArrayList<ArrayList<KeyValue>> join = arrayListDeferred.join();
            cache.addAll(join);
            return next();
        } catch (Exception e) {
            scanner.close();
            throw Exceptions.getIOException(e);
        }
    }

    private static final Function<org.hbase.async.KeyValue,org.apache.hadoop.hbase.KeyValue> toHbaseFunction= new Function<KeyValue, org.apache.hadoop.hbase.KeyValue>() {
        @Override
        public org.apache.hadoop.hbase.KeyValue apply(@Nullable KeyValue input) {
            return new org.apache.hadoop.hbase.KeyValue(input.key(),input.family(),input.qualifier(),input.timestamp(),input.value());
        }
    };

    private Result newResult(List<KeyValue> keyValues) {
        List<org.apache.hadoop.hbase.KeyValue> hbaseKvs = Lists.transform(keyValues,toHbaseFunction);
        return new Result(hbaseKvs);
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
    }
}
