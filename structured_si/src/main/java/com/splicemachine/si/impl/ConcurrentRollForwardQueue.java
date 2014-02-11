package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.si.api.RollForwardQueue;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RollForward queue which uses non-blocking concurrency controls.
 *
 * This implementation makes a Best-effort attempt at rolling forward rows
 * in a relatively timely manner, in the following ways:
 *
 * There is a fixed, bounded heap that the queue is allowed to grow to. Once this
 * heap is exceeded (or the specified max number of queue entries is exceeded), then
 * a roll-forward action is asynchronously triggered by offloading all queue entries
 * to another thread for later processing. To avoid excessive memory use, the number
 * of delayed roll-forward events is bounded. Once both are exceeded, then
 * batches of records are discarded (to be rolled forward later by read events).
 *
 * Thus, if the max queue heap is 2MB, and the maximum  number of concurrent roll-forward
 * events is 10, then at any point in time, no more than 20MB of heap is occupied by
 * this roll-forward action. This can be global (if the background thread pool and thread queue
 * is the same for all RollForwardQueue instances), or local (if a separate pool is used
 * for each instance).
 *
 * This implementation also specifies a maximum time that a queue can remain in activity
 * without being rolled forward. Thus, after a timeout has exceeded, rows will be rolled
 * forward even if there are not enough to fill out the heap size and/or max entries control;
 *
 * This implementation is thread-safe. It uses non-blocking mechanisms wherever possible to
 * ensure thread-safety.
 *
 * @author Scott Fines
 * Created on: 8/21/13
 */
public class ConcurrentRollForwardQueue implements RollForwardQueue<byte[],ByteBuffer> {
    private static final Logger LOG = Logger.getLogger(ConcurrentRollForwardQueue.class);

    private final Hasher<byte[],ByteBuffer> hasher;

    private final RollForwardAction<byte[]> action;

    private final long maxHeapSize;
    private final AtomicLong currentHeapSize = new AtomicLong(0l);
    private final int maxEntries;
    private final AtomicInteger currentSize = new AtomicInteger(0);
    private final long timeout;

    private final ScheduledExecutorService timedRollerPool;
    private final ExecutorService filledRollerPool;

    private final ConcurrentLinkedQueue<LongEntry<byte[]>> currentBuffer;

    private volatile boolean shutdown = false;

    public ConcurrentRollForwardQueue(Hasher<byte[],ByteBuffer> hasher,
                                       RollForwardAction<byte[]> action,
                                       long maxHeapSize,
                                       int maxEntries,
                                       long timeoutMs,
                                       ScheduledExecutorService timedRollerPool,
                                       ExecutorService filledRollerPool){
        this.hasher = hasher;
        this.action = action;
        this.maxEntries = maxEntries;
        this.maxHeapSize = maxHeapSize;
        this.timedRollerPool = timedRollerPool;
        this.filledRollerPool = filledRollerPool;
        this.timeout = timeoutMs;

        this.currentBuffer = new ConcurrentLinkedQueue<LongEntry<byte[]>>();
    }

    public void start(){
        /*
         * Schedules a rollforward at a fixed rate. This ensures that the longest an entry will
         * remain in the queue is {@code timeout}--that is, for very small writes, the roll forward
         * will wait only so long before rolling them forward regardless.
         *
         * It may seem a bit weird that we put no guards in to prevent this from going during a high load
         * situation. This means that NO MATTER WHAT, a RollForward event will occur every {@code timeout} ms,
         * regardless of whether or not there is heavy activity. It is possible that this RollForward event
         * will be issues simultaneously with other RollForward events.  This results in duplicate
         * work occurring (potentially), but does not require any form of communication between recording
         * threads and background threads to maintain.
         */
        timedRollerPool.schedule(new ScheduledRoll(), timeout, TimeUnit.MILLISECONDS);
    }

    public void stop(){
        shutdown = true;
    }

    @Override
    public void recordRow(long transactionId, byte[] rowKey, Boolean knownToBeCommitted) {
        /*
         * First, we increment our counters, and determine if a rollforward event
         * should be produced. If it needs to be produced (i.e. heap or entries are exceeded),
         * then the current buffer is drained, and a roll forward action is produced.
         *
         * Finally, this element is added to the set
         */
        int entries = currentSize.incrementAndGet();
        long heap = currentHeapSize.addAndGet(rowKey.length);
        if(entries>maxEntries||heap>maxHeapSize){
            rollForward(entries,heap);
        }
        LongEntry<byte[]> entry = new LongEntry<byte[]>(transactionId,rowKey);
        currentBuffer.add(entry);
    }

    @Override
    public int getCount() {
        return currentSize.get();
    }

    /*private helper classes and methods*/
    private void rollForward(int maxEntries,long maxHeap) {
        /*
         * Initiate a RollForward with entries up to maxEntries, and a heap up to maxHeap.
         *
         * Note that there is no blocking performed during this step. It is highly possible that
         * multiple threads will simultaneously decide that a flush is necessary. In that case, they
         * will each submit a RollForward action for execution. This may result in more work being done
         * than is strictly necessary(multiple threads may be attempting to roll forward the same
         * transaction), but prevents us from having to block to flush.
         */
        //a rollforward event is required, so flush one
        Map<Long,Set<ByteBuffer>> itemsToRoll = new HashMap<Long,Set<ByteBuffer>>(maxEntries);
        int rolledItemCount=0;
        long heapSize = 0l;

        while(rolledItemCount<maxEntries&&heapSize<maxHeap){
            LongEntry<byte[]> entry = currentBuffer.poll();
            //if there are no more items in the queue, then we're good. Just flush what we have
            if(entry==null)
                break;

            Set<ByteBuffer> entrySet = itemsToRoll.get(entry.entry);
            if(entrySet==null){
                entrySet = Sets.newHashSetWithExpectedSize(1);
                itemsToRoll.put(entry.entry,entrySet);
            }
            entrySet.add(hasher.toHashable(entry.value));
            rolledItemCount++;
            heapSize+=entry.value.length;
        }

        //reset our counters to allow others to buffer in
        currentSize.addAndGet(-rolledItemCount);
        currentHeapSize.addAndGet(-heapSize);

        filledRollerPool.submit(new RollForward(itemsToRoll,action));
    }

    private static class LongEntry<V>{
        private final long entry;
        private final V value;

        private LongEntry(long entry, V value) {
            this.entry = entry;
            this.value = value;
        }

        public long getEntry() {
            return entry;
        }

        public V getValue() {
            return value;
        }
    }

    private class RollForward implements Callable<Void> {
        private final Map<Long,Set<ByteBuffer>> itemsToRoll;
        private final RollForwardAction<byte[]> action;
        public RollForward(Map<Long, Set<ByteBuffer>> itemsToRoll,RollForwardAction<byte[]> action) {
            this.action = action;
            this.itemsToRoll = itemsToRoll;
        }

        @Override
        public Void call() throws Exception {
            for(Map.Entry<Long,Set<ByteBuffer>> itemToRoll:itemsToRoll.entrySet()){
                Set<ByteBuffer> buffer = itemToRoll.getValue();
                long txnId = itemToRoll.getKey();
                List<byte[]> items = Lists.newArrayListWithCapacity(buffer.size());
                for(ByteBuffer bufferItem:buffer){
                    items.add(hasher.fromHashable(bufferItem));
                }

                try{
                    action.rollForward(txnId,items);
                }catch(WrongRegionException e){
                    if(LOG.isDebugEnabled()){
                        LOG.debug("WrongRegionException encountered while rolling forward");
                    }
                }catch(IOException ioe){
                    LOG.warn("Encountered error while rolling forward",ioe);
                }catch(Exception e){
                    LOG.warn("Encountered error while rolling forward",e);
                }
            }
            return null;
        }
    }

    private class ScheduledRoll implements Runnable{
            @Override
            public void run() {
                if(shutdown) return;

                int currentEntries = currentSize.get();
                long heapSize =currentHeapSize.get();

                if(currentEntries>0)
                    rollForward(currentEntries,heapSize);

                if(!shutdown)
                    timedRollerPool.schedule(new ScheduledRoll(),timeout,TimeUnit.MILLISECONDS);
            }
    }
}
