package com.splicemachine.hbase.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Thread-safe Reusable buffer for batching expensive operations.
 *
 * @author Scott Fines
 * Created on: 3/8/13
 */
public class ThreadSafeCallBuffer<E> implements CallBuffer<E> {
    private static final int BUFFER_FULL_CHECK_PERIOD = 10; // check buffer size every ten adds in bulk

    private final BlockingQueue<E> queue;
    private final Listener<E> listener;
    private final long maxHeapSize;
    private final int maxBufferEntries;
    private final AtomicLong currentHeapSize = new AtomicLong(0l);

    public ThreadSafeCallBuffer(Listener<E> listener, long maxHeapSize, int maxBufferEntries) {
        if(maxHeapSize>0)
            this.maxHeapSize = maxHeapSize;
        else
            this.maxHeapSize = Long.MAX_VALUE; //never flush because of heap

        this.maxBufferEntries = maxBufferEntries;
        this.listener = listener;
        if(maxBufferEntries>0)
            queue = new LinkedBlockingQueue<E>(maxBufferEntries);
        else
            queue = new LinkedBlockingQueue<E>();
    }

    @Override
    public void add(E element) throws Exception{
        /*
         * Thread safety discussion:
         *
         * When the buffer is not full, many threads will safely access this
         * method without blocking or interfering with one another.
         *
         * The real interesting bits is what happens when we get close to either
         * A) the max number of buffer entries
         * B) the max heap size
         *
         * If the max number of buffer entries is hit, then all threads will
         * block until one thread can successfully drain the queue to contain
         * enough space for the threads to continue--e.g. this method is perfectly
         * safe in that scenario, and the buffer will never grow beyond the specified max
         * number of entries.
         *
         * However, if the max heap is hit close enough together, it is possible that
         * two threads will add two entries to the queue, and both threads would have
         * pushed the buffer over it's limit on their own. In that case, both
         * threads will attempt to drain the queue and inform the listener of the flush. However,
         * the drainTo() action is atomic, so one thread will get a full collection and the
         * other will get an empty one. The one who gets and empty one will just give up and
         * not worry about it, while the full one will inform the listener, making this
         * method thread-safe. However, for a short time, the size of the buffer will exceed
         * that of the max heap.
         */
        //compute the heap size of the element
        long heap = listener.heapSize(element);
        //add the element to the queue
        queue.add(element);

        //compute the total heap of the queue
        long totalHeap = currentHeapSize.addAndGet(heap);

        if(totalHeap>= maxHeapSize || queue.size()>=maxBufferEntries){
            flushBuffer();
        }
    }

    @Override
    public void addAll(E[] elements) throws Exception{
        int n=0;
        for(E element: elements){
            long heap = listener.heapSize(element);
            queue.add(element);
            long totalHeap = currentHeapSize.addAndGet(heap);

            n++;
            if(queue.size()>=maxBufferEntries||(n %BUFFER_FULL_CHECK_PERIOD==0 && totalHeap>=maxHeapSize)){
                flushBuffer();
            }
        }
    }

    @Override
    public void addAll(Collection<? extends E> elements) throws Exception{
        int n=0;
        for(E element: elements){
            long heap = listener.heapSize(element);
            queue.add(element);
            long totalHeap = currentHeapSize.addAndGet(heap);

            n++;
            if(queue.size()>=maxBufferEntries||(n %BUFFER_FULL_CHECK_PERIOD==0 && totalHeap>=maxHeapSize)){
                flushBuffer();
            }
        }
    }

    @Override
    public void flushBuffer()  throws Exception{
        List<E> entries = new ArrayList<E>(queue.size());
        long currentHeap = currentHeapSize.get();
        queue.drainTo(entries);
        currentHeapSize.addAndGet(-1l*currentHeap);
        if(entries.size()<=0) return; //no worries, someone else is doing the drain
        listener.bufferFlushed(entries,this);
    }

    @Override
    public void close() throws Exception{
        //nothing to do here
        flushBuffer();
    }
}
