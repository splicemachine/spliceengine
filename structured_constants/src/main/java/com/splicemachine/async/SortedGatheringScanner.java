package com.splicemachine.async;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.collections.NullStopIterator;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.*;
import com.splicemachine.metrics.Timer;
import com.splicemachine.stream.BaseCloseableStream;
import com.splicemachine.stream.CloseableStream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.SpliceLogUtils;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An asynchronous, vectored scanner that returns data in sorted order.
 *
 * @author Scott Fines
 * Date: 7/30/14
 */
public class SortedGatheringScanner implements AsyncScanner{
    private static final Logger LOG = Logger.getLogger(SortedGatheringScanner.class);
    private final Timer timer;
    private final Counter remoteBytesCounter;
    private final Comparator<byte[]> sortComparator;

    private final SubScanner[] scanners;
    private List<KeyValue>[] nextAnswers;
    private boolean[] exhaustedScanners;
    private Callable<Void>[] cleanups = null;
    
    public static AsyncScanner newScanner(int maxQueueSize,
                                          MetricFactory metricFactory,
                                          Function<Scan, Scanner> conversionFunction,
                                          List<Scan> scans,
                                          Comparator<byte[]> sortComparator,
                                          Callable<Void>... cleanupActions) throws IOException {
        List<Scanner> scanners = Lists.transform(scans, conversionFunction);
        return new SortedGatheringScanner(scanners,maxQueueSize,sortComparator,metricFactory,cleanupActions);
    }

    public SortedGatheringScanner(List<Scanner> scanners, int maxQueueSize, Comparator<byte[]> sortComparator, MetricFactory metricFactory, Callable<Void>... cleanupActions) {
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();
        if(sortComparator==null)
            this.sortComparator = Bytes.BYTES_COMPARATOR;
        else
            this.sortComparator = sortComparator;

        this.scanners = new SubScanner[scanners.size()];
        for(int i=0;i<scanners.size();i++){
            Scanner scanner = scanners.get(i);
            //each scanner gets 1/n the size of the queue
            this.scanners[i] = new SubScanner(maxQueueSize/scanners.size(), scanner);
        }
        //noinspection unchecked
        this.nextAnswers = new List[scanners.size()];
        this.exhaustedScanners = new boolean[scanners.size()];
        this.cleanups = cleanupActions;
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
    public CloseableStream<List<KeyValue>> stream() {
        return new BaseCloseableStream<List<KeyValue>>() {
            @Override public void close() throws IOException { SortedGatheringScanner.this.close(); }

            @Override
            public List<KeyValue> next() throws StreamException {
                try {
                    return nextKeyValues();
                } catch (Exception e) {
                    throw new StreamException(e);
                }
            }
        };
    }

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

        if (cleanups == null) return;
        
        for (Callable<Void> cleanup : cleanups) {
			try {
				cleanup.call();
			} catch (Exception e) {
            	// Can't override close() to throw IOException, so log error
                SpliceLogUtils.error(LOG, String.format("Failure while trying to call a cleanup action: %s", cleanup), e);
			}
        }
	}

    @Override
    public List<KeyValue> nextKeyValues() throws Exception {
        timer.startTiming();
        List<KeyValue> currMinList = null;
        KeyValue currMinFirst = null;
        int currMinPos = -1;
        for(int i=0;i<nextAnswers.length;i++){
            if(exhaustedScanners[i]) continue;

            List<KeyValue> next;
            if(nextAnswers[i]!=null)
                next = nextAnswers[i];
            else{
                /*
                 * We used this value last time, make sure it's filled
                 */
                next = scanners[i].next();
                if(next==null || next.size()<=0){
                    exhaustedScanners[i] = true;
                    continue;
                }
                nextAnswers[i] = next;
            }

            if(currMinFirst==null){
                currMinFirst = next.get(0);
                currMinList = next;
                currMinPos = i;
            }else{
                KeyValue first = next.get(0);
                if(sortComparator.compare(first.key(),currMinFirst.key())<0){
                    currMinList = next;
                    currMinFirst = first;
                    currMinPos = i;
                }
            }
        }
        if(currMinFirst==null)
            timer.stopTiming();
        else{
            timer.tick(1);
            nextAnswers[currMinPos] = null;
        }
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

            try {
                List<KeyValue> take = resultQueue.take();
                if(take==POISON_PILL) //the scanner finished, but there's nothing left
                    return null;
                peeked = take;
                if(!done && request==null){
                    request = scanner.nextRows().addCallback(this); //initiate a new request
                }
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
                resultQueue.offer(POISON_PILL); //make sure that poison_pill is on the queue
                done = true;
                scanner.close();
                request = null;
                return null;
            }

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
