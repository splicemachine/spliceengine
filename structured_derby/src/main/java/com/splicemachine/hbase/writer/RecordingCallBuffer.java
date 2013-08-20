package com.splicemachine.hbase.writer;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Created on: 8/19/13
 */
public class RecordingCallBuffer<E> implements CallBuffer<E>{
    private final CallBuffer<E> delegate;

    private long totalElementsAdded = 0l;
    private long totalBytesAdded = 0l;
    private long totalFlushes = 0l;
    private final Listener<E> listener;

    public RecordingCallBuffer(CallBuffer<E> delegate,Listener<E> listener) {
        this.delegate = delegate;
        this.listener = listener;
    }

    @Override
    public void add(E element) throws Exception {
        totalElementsAdded+=1;
        totalBytesAdded+=listener.heapSize(element);
        delegate.add(element);
    }

    @Override
    public void addAll(E[] elements) throws Exception {
        delegate.addAll(elements);
    }

    @Override
    public void addAll(Collection<? extends E> elements) throws Exception {
        delegate.addAll(elements);
    }

    @Override
    public void flushBuffer() throws Exception {
        totalFlushes++;
        delegate.flushBuffer();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    public long getTotalElementsAdded(){
        return totalElementsAdded;
    }

    public long getTotalBytesAdded(){
        return totalBytesAdded;
    }

    public long getTotalFlushes(){
        return totalFlushes;
    }

    public double getAverageEntriesPerFlush(){
        return totalElementsAdded/(double)totalFlushes;
    }

    public double getAverageSizePerFlush(){
        return totalBytesAdded/(double)totalFlushes;
    }

    public CallBuffer<E> unwrap(){
        return delegate;
    }
}
