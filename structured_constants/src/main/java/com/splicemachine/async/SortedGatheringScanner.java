package com.splicemachine.async;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.collections.NullStopIterator;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.metrics.*;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.Timer;
import com.splicemachine.stream.BaseCloseableStream;
import com.splicemachine.stream.CloseableStream;
import com.splicemachine.stream.StreamException;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
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

    public static AsyncScanner newScanner(Scan baseScan,int maxQueueSize,
                                          MetricFactory metricFactory,
                                          Function<Scan,Scanner> conversionFunction,
                                          RowKeyDistributor rowKeyDistributor,
                                          Comparator<byte[]> sortComparator) throws IOException {

        Scan[] distributedScans = rowKeyDistributor.getDistributedScans(baseScan);
//        if(distributedScans.length<=1){
//            return new QueuedAsyncScanner(conversionFunction.apply(distributedScans[0]),metricFactory,maxQueueSize);
//        }

        List<Scanner> scans = Lists.newArrayListWithExpectedSize(distributedScans.length);
        for(Scan scan:distributedScans){
            scans.add(conversionFunction.apply(scan));
        }

        return new SortedGatheringScanner(scans,maxQueueSize,sortComparator,metricFactory);
    }

    public SortedGatheringScanner(List<Scanner> scanners, int maxQueueSize, Comparator<byte[]> sortComparator,MetricFactory metricFactory){
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

    public static void main(String...args) throws Exception{
        Logger.getRootLogger().addAppender(new ConsoleAppender(new SimpleLayout()));
        Logger.getRootLogger().setLevel(Level.INFO);
        Scan baseScan = new Scan();
        byte[] startRow = Bytes.toBytesBinary("\\x90\\xF4y\\x1D\\xBF\\xE9\\xF0\\x01");
        baseScan.setStartRow(startRow);
        baseScan.setStopRow(BytesUtil.unsignedCopyAndIncrement(startRow));

        RowKeyDistributorByHashPrefix.Hasher hasher = new RowKeyDistributorByHashPrefix.Hasher(){

            @Override
            public byte[] getHashPrefix(byte[] originalKey) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[][] getAllPossiblePrefixes() {
                byte[][] buckets = new byte[16][];
                for(int i=0;i<buckets.length;i++){
                    buckets[i] = new byte[]{(byte)(i*0xF0)};
                }
                return buckets;
            }

            @Override
            public int getPrefixLength(byte[] adjustedKey) {
                return 1;
            }
        };
        RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(hasher);

        final HBaseClient client = SimpleAsyncScanner.HBASE_CLIENT;
        try{
            AsyncScanner scanner = SortedGatheringScanner.newScanner(baseScan,1024,
                    Metrics.noOpMetricFactory(), new Function<Scan, Scanner>() {
                @Nullable
                @Override
                public Scanner apply(@Nullable Scan scan) {
                    Scanner scanner = client.newScanner(SpliceConstants.TEMP_TABLE_BYTES);
                    scanner.setStartKey(scan.getStartRow());
                    byte[] stop = scan.getStopRow();
                    if(stop.length>0)
                        scanner.setStopKey(stop);
                    return scanner;
                }
            },keyDistributor,null);
            scanner.open();

            Result r;
            while((r = scanner.next())!=null){
                System.out.println(Bytes.toStringBinary(r.getRow()));
            }
        }finally{
            client.shutdown().join();
        }
    }
}
