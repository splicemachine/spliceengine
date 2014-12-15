package com.splicemachine.async;

import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.utils.SpliceLogUtils;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Efficient single-threaded AsyncScanner implementation.
 *
 * @author Scott Fines
 *         Date: 12/15/14
 */
public class QueueingAsyncScanner extends BaseAsyncScanner{
    private static final Logger LOG = Logger.getLogger(QueueingAsyncScanner.class);
    private final Scanner scanner;
    private final BlockingQueue<List<KeyValue>> buffer;
    private final int maxQueueSize;
    private volatile Deferred<ArrayList<ArrayList<KeyValue>>> outstandingRequest;

    private volatile boolean closed = false;
    private volatile boolean done = false;

    public QueueingAsyncScanner(Scanner scanner, MetricFactory metricFactory) {
        this(scanner,new LinkedBlockingQueue<List<KeyValue>>(),5*scanner.getMaxNumRows(),metricFactory);
    }

    public QueueingAsyncScanner(Scanner scanner, int maxQueueSize,MetricFactory metricFactory) {
        this(scanner,new LinkedBlockingQueue<List<KeyValue>>(),maxQueueSize,metricFactory);
    }

    public QueueingAsyncScanner(Scanner scanner,
                                BlockingQueue<List<KeyValue>> buffer,
                                int maxQueueSize,
                                MetricFactory metricFactory) {
        super(metricFactory);
        this.scanner = scanner;
        this.buffer = buffer;
        this.maxQueueSize = maxQueueSize;
    }

    @Override
    public List<KeyValue> nextKeyValues() throws Exception {
        assert !closed: "Scanner already closed!";
        timer.startTiming();
        submitNewRequestIfPossible();

        List<KeyValue> kvs = buffer.poll();
        if(kvs==null && outstandingRequest!=null) {
            outstandingRequest.join();
            kvs = buffer.poll();
        }

        if(kvs==null || kvs.size()<=0) {
            timer.stopTiming();
            return null;
        }else{
            timer.tick(1);
            if(remoteBytesCounter.isActive()){
                countKeyValues(kvs);
            }
        }
        return kvs;
    }

    @Override
    public void open() throws IOException {
        outstandingRequest = scanner.nextRows().addCallback(callback);
    }

    @Override
    public void close() {
        if(closed) return; //no-op if already been closed
        closed=true;
        try{
            if(outstandingRequest!=null)
                outstandingRequest.join();

            scanner.close().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private final Callback<ArrayList<ArrayList<KeyValue>>,ArrayList<ArrayList<KeyValue>>> callback
            = new Callback<ArrayList<ArrayList<KeyValue>>, ArrayList<ArrayList<KeyValue>>>() {
        @Override
        public ArrayList<ArrayList<KeyValue>> call(ArrayList<ArrayList<KeyValue>> arg) throws Exception {
            if(arg==null || arg.size()<=0){
                done=true;
            }else {
                for (int i = 0; i < arg.size(); i++) {
                    buffer.offer(arg.get(i));
                }
                SpliceLogUtils.trace(LOG, "Added %d rows to buffer", arg.size());
            }

            //indicate we are ready for the next submission
            outstandingRequest = null;
            return null;
        }
    };

    private void countKeyValues(List<KeyValue> kvs) {
        for(int i=0;i<kvs.size();i++){
            KeyValue kv = kvs.get(i);
            remoteBytesCounter.add(kv.key().length+kv.value().length+kv.qualifier().length+kv.family().length);
        }
    }

    private void submitNewRequestIfPossible() {
        if(done) return;
        if(outstandingRequest==null && buffer.size()<maxQueueSize){
            SpliceLogUtils.trace(LOG,"Issuing next request");
            //we should issue the next request only if our buffer doesn't have very much data in it
            outstandingRequest = scanner.nextRows().addCallback(callback);
        }
    }
}
