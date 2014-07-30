package com.splicemachine.hbase.async;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.stats.*;
import com.splicemachine.utils.NullStopIterator;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An asynchronous, vectored scanner that returns data in sorted order.
 *
 * @author Scott Fines
 * Date: 7/30/14
 */
public class SortedGatheringScanner implements AsyncScanner{
    private final Timer timer;
    private final Counter remoteBytesCounter;

    private final SubScanner[] scanners;
    private List<KeyValue>[] nextAnswers;
    private boolean[] exhaustedScanners;

    public static AsyncScanner newScanner(Scan baseScan,int maxQueueSize,
                                          MetricFactory metricFactory,
                                          Function<Scan,Scanner> conversionFunction,
                                          RowKeyDistributor rowKeyDistributor) throws IOException {

        Scan[] distributedScans = rowKeyDistributor.getDistributedScans(baseScan);
        if(distributedScans.length<=1){
            return new SimpleAsyncScanner(conversionFunction.apply(baseScan),metricFactory);
        }

        List<Scanner> scans = Lists.newArrayListWithExpectedSize(distributedScans.length);
        for(Scan scan:distributedScans){
            scans.add(conversionFunction.apply(scan));
        }

        return new GatheringScanner(scans,maxQueueSize,metricFactory);
    }

    public SortedGatheringScanner(List<Scanner> scanners, int maxQueueSize, MetricFactory metricFactory){
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();

        this.scanners = new SubScanner[scanners.size()];
        for(int i=0;i<scanners.size();i++){
            Scanner scanner = scanners.get(i);
            //each scanner gets 1/n the size of the queue
            this.scanners[i] = new SubScanner(maxQueueSize/scanners.size(), scanner);
        }
        //noinspection unchecked
        this.nextAnswers = new List[scanners.size()];
        this.exhaustedScanners = new boolean[scanners.size()];
    }

    @Override
    public void open() throws IOException {
        for(SubScanner scanner:scanners){
            scanner.open();
        }
    }

    @Override public TimeView getRemoteReadTime() { return timer.getTime(); }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return timer.getNumEvents(); }
    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0; }
    @Override public long getLocalRowsRead() { return 0; }

    @Override
    public Result next() throws IOException {
        try {
            List<KeyValue> kvs = nextKeyValues();
            if(kvs==null||kvs.size()<=0) return null;

            return new Result(AsyncScannerUtils.convertFromAsync(kvs));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        List<Result> results = Lists.newArrayListWithExpectedSize(nbRows);
        Result r;
        while((r = next())!=null){
            results.add(r);
        }
        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close() {
        for(SubScanner scanner:scanners){
            scanner.close();
        }
    }

    @Override
    public List<KeyValue> nextKeyValues() throws Exception {
        timer.startTiming();
        List<KeyValue> currMinList = null;
        KeyValue currMinFirst = null;
        for(int i=0;i<nextAnswers.length;i++){
            if(exhaustedScanners[i]) continue;

            if(nextAnswers[i]==null){
                /*
                 * We used this value last time, make sure it's filled
                 */
                List<KeyValue> next = scanners[i].next();
                if(next==null || next.size()<=0){
                    exhaustedScanners[i] = true;
                    continue;
                }
                nextAnswers[i] = next;
                if(currMinFirst==null){
                    currMinFirst = next.get(0);
                    currMinList = next;
                }else{
                    KeyValue first = next.get(0);
                    if(Bytes.compareTo(first.key(),currMinFirst.key())<0){
                        currMinList = next;
                        currMinFirst = first;
                    }
                }
            }
        }
        if(currMinFirst==null)
            timer.stopTiming();
        else
            timer.tick(1);
        return currMinList;
    }


    @Override
    public Iterator<Result> iterator() {
        return new NullStopIterator<Result>() {
            @Override protected Result nextItem() throws IOException { return SortedGatheringScanner.this.next(); }
            @Override public void close() throws IOException { SortedGatheringScanner.this.close(); }
        };
    }

    private static final List<KeyValue> POISON_PILL = Collections.emptyList();
    private static class SubScanner implements Callback<Void,ArrayList<ArrayList<KeyValue>>>{

        private final BlockingQueue<List<KeyValue>> resultQueue;
        private final int maxQueueSize;
        private final Scanner scanner;

        private List<KeyValue> peeked;

        private volatile boolean done;
        private volatile Deferred<Void> request = null;

        private SubScanner(int maxQueueSize, Scanner scanner) {
            this.maxQueueSize = maxQueueSize;
            this.scanner = scanner;
            this.resultQueue = new LinkedBlockingQueue<List<KeyValue>>();
        }

        private List<KeyValue> peekNext() throws IOException{
            if(peeked!=null) return peeked;
            if(done) return null;
            if(request==null)
                request = scanner.nextRows().addCallback(this);

            try {
                List<KeyValue> take = resultQueue.take();
                if(take==POISON_PILL) //the scanner finished, but there's nothing left
                    return null;
                peeked = take;
                return take;
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        private List<KeyValue> next() throws IOException{
            List<KeyValue> n = peekNext();
            peeked = null;
            return n;
        }

        @Override
        public Void call(ArrayList<ArrayList<KeyValue>> arg) throws Exception {
            if(arg==null || done){
                resultQueue.offer(POISON_PILL);
                done = true;
                return null;
            }
            resultQueue.addAll(arg);
            if(resultQueue.size()>=maxQueueSize){
                request = null;
                return null;
            }

            if(scanner.onFinalRegion() && arg.size()<scanner.getMaxNumRows()){
                done = true;
                scanner.close();
                return null;
            }
            request = scanner.nextRows().addCallback(this);

            return null;
        }

        public void open() {
            request = scanner.nextRows().addCallback(this);
        }

        public void close() {
            done = true;
            scanner.close();
        }
    }
}
