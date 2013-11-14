package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.utils.ConcurrentRingBuffer;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Buffers Region writes.
 *
 * There are two scalability concerns with the raw HBase RegionScanner.
 *
 * The first is that reads aren't buffered, which means each read must access the Memstore directly,
 * which may require additional IO (disk seeks, etc.).
 *
 * The second is that each call to regionScanner.next() involves a synchronization step--a read lock
 * must be acquired by the region, and then released. Since we may potentially be doing this over a
 * large portion of the table, this results in a large number of locks and unlocks.
 *
 * One naive way to resolve the locking issue is to just acquire a read lock with the first row in
 * the scan, then release it after the last read has completed. However, this causes additional
 * scalability issues: the read lock that is acquired will block any attempts to write to that
 * region until the scan has completed, which may be a very long time. This will murder attempts
 * to provide reasonable update performance for most tables which are under heavy read load, and is
 * also generally a bad idea.
 *
 * To make matters worse, there is only one thread which will call next() at a time, <em>but it isn't
 * always the same thread!</em> This means that, even though we know that only one thread will be
 * calling our code at a time (which would otherwise not require synchronization), we are still
 * communicating information across multiple threads (in the form of read state, etc.) and thus
 * must be thread safe.
 *
 * So, to recap: we must have a region scanner which has the following properties:
 *
 * 1. Thread-safe
 * 2. Buffering (hold a buffer of results in memory so that repeated access is not required)
 * 3. Does not hold a region lock for longer than is necessary.
 * 4. Amortized nonblocking (e.g. nonblocking <em>most of the time</em>)
 *
 * This class provides such an implementation, mostly by using a {@link ConcurrentRingBuffer}
 * as our buffering abstraction.
 *
 * @author Scott Fines
 * Created on: 7/25/13
 */
public class BufferedRegionScanner implements RegionScanner{
    private final RegionScanner delegate;

    private ConcurrentRingBuffer<List<KeyValue>> ringBuffer;

    @SuppressWarnings("unchecked")
	public BufferedRegionScanner(HRegion region,RegionScanner delegate,int bufferSize) {
        this.delegate = delegate;
        List<KeyValue>[] template = new List[bufferSize];
        this.ringBuffer = new ConcurrentRingBuffer<List<KeyValue>>(bufferSize,template,new ReadFiller(delegate,region));
    }

    @Override
    public HRegionInfo getRegionInfo() {
        return delegate.getRegionInfo();
    }

    @Override
    public boolean isFilterDone() {
        return delegate.isFilterDone();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return delegate.reseek(row);
    }

    @Override
    public long getMvccReadPoint() {
        return delegate.getMvccReadPoint();
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
        return next(result);
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
        return next(result);
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
       /*
        * The basic HBase next(results) method generally does the following steps:
        *
        * 1. acquire a read lock
        * 2. check if region is closed (exploding if it is)
        * 3. set the thread read point
        * 4. do some reading of data
        * 5. release the lock
        *
        * through the HBase api, this looks like
        *
        * region.startRegionOperation();
        * try{
        *   MultiVersionConsistencyControl.setThreadReadPoint(regionScanner.getMvccReadPoint());
        *
        *  //read data using regionScanner.nextRaw();
        * }finally{
        *  region.stopRegionOperation();
        * }
        *
        * Which is usually performed under a synchronization block itself,
        * but since we have to use our buffer where possible, our version must be different.
        *
        * Since out buffer abstraction takes care of coordinating buffer fills and such, we only need
        * to make sure that our filler properly synchronizes the actual reads from HBase into the buffer.
        *
        */
        try {
            List<KeyValue> next = ringBuffer.next();
            if(next!=null){
                results.addAll(next);
                return true;
            }else return false;
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if(t instanceof IOException)
                throw (IOException)t;
            else
                throw new IOException(t);
        }
    }


    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
        return next(results);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        return next(result);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
        return next(result);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static class ReadFiller implements ConcurrentRingBuffer.Filler<List<KeyValue>> {
        private final RegionScanner scanner;
        private final HRegion region;
        private long count;

        public ReadFiller(RegionScanner scanner, HRegion region) {
            this.scanner = scanner;
            this.region = region;
        }

        @Override
        public void prepareToFill() throws ExecutionException {
            try {
                region.startRegionOperation();
                count = 0;
            } catch (NotServingRegionException e) {
                throw new ExecutionException(e);
            } catch (RegionTooBusyException e) {
                throw new ExecutionException(e);
            } catch (InterruptedIOException e) {
                throw new ExecutionException(e);
            }

            MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
        }

        @Override
        public List<KeyValue> getNext(List<KeyValue> old) throws ExecutionException {
            if(old==null)
                old = Lists.newArrayListWithCapacity(2);
            else
                old.clear();
            try {
                scanner.nextRaw(old,null);
                count++;
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
            return old.isEmpty()?null:old; // We have a null check on the get next and not an empty...
        }

        @Override
        public void finishFill() {
        	HRegionUtil.updateReadRequests(region,count);
            region.closeRegionOperation();
        }
    }
}
