package com.splicemachine.hbase.writer;

import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.ObjectArrayList;

/**
 * Thread-unsafe implementation of a CallBuffer. Useful for single-threaded operations,
 * where thread-safety isn't important when writing to the buffer.
 *
 * @author Scott Fines
 * Created on: 3/18/13
 */
public class UnsafeCallBuffer<E> implements CallBuffer<E>{
    private static final int BUFFER_SIZE_CHECK=10;
    private final ObjectArrayList<E> buffer;
    private final Listener<E> listener;
    private long currentHeapSize;

    private final BufferConfiguration bufferConfiguration;

    public UnsafeCallBuffer(BufferConfiguration bufferConfiguration,Listener<E> listener) {
        this.listener = listener;
        this.bufferConfiguration = bufferConfiguration;

        if(bufferConfiguration.getMaxEntries()>0)
            buffer = new ObjectArrayList<E>(bufferConfiguration.getMaxEntries());
        else
            buffer = new ObjectArrayList<E>();
    }

    @Override
    public void add(E element) throws Exception {

        currentHeapSize+=listener.heapSize(element);
        buffer.add(element);

        checkBuffer();
    }

    @Override
    public void addAll(E[] elements) throws Exception {
        int n=0;
        for(E element:elements){
            n++;
            currentHeapSize+=listener.heapSize(element);
            buffer.add(element);

            if(n%BUFFER_SIZE_CHECK==0){
                checkBuffer();
            }
        }
        checkBuffer();
    }

    @Override
    public void addAll(ObjectArrayList<E> elements) throws Exception {
        int n=0;
        Object[] elementsBuffer = elements.buffer;
        int size = elements.size();
        for (int i = 0; i< size; i++) {
        	E element = (E) elementsBuffer[i];
            n++;
            currentHeapSize+=listener.heapSize(element);
            buffer.add(element);

            if(n%BUFFER_SIZE_CHECK==0){
                checkBuffer();
            }
        }
        checkBuffer();
    }

    @Override
    public void flushBuffer() throws Exception {
        if(buffer.size()<=0) return; //nothing to do

        ObjectArrayList<E> elements = ObjectArrayList.from(buffer);
        buffer.clear();
        currentHeapSize=0l;
        listener.bufferFlushed(elements,this);
    }

    @Override
    public void close() throws Exception {
        flushBuffer();
    }

    private void checkBuffer() throws Exception {
        int maxEntries = bufferConfiguration.getMaxEntries();
        if(currentHeapSize>bufferConfiguration.getMaxHeapSize()||(maxEntries>0 && buffer.size()>maxEntries))
            flushBuffer();
    }
}
