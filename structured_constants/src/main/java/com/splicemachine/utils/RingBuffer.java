package com.splicemachine.utils;

import java.util.Arrays;

/**
 * Simple non-thread-safe ring buffer.
 *
 * If you need a thread-safe ring buffer, use the LMAX disruptor instead.
 *
 * @author Scott Fines
 * Date: 7/22/14
 */
public class RingBuffer<T> {
    private final int mask;
    private Object[] buffer;
    private int writePosition;
    private int readPosition;

    private int offsetReadPosition; //offset for readReset()

    public RingBuffer(int bufferSize) {
        int s = 1;
        while(s<bufferSize)
            s<<=1;
        this.buffer = new Object[s];
        this.mask = s-1;
        writePosition = 0;
        readPosition = 0;
    }

    /**
     * @return the next item in the buffer, or {@code null} if there are
     * no items in the buffer
     */
    @SuppressWarnings("unchecked")
    public T next(){
        if(readPosition>=writePosition) return null; //buffer is currently empty
        int pos = readPosition & mask;
        T n = (T)buffer[pos];
        readPosition++;
        return n;
    }

    public void add(T element){
        int pos = writePosition & mask;
        buffer[pos] = element;
        writePosition++;
    }

    public int size(){
        return writePosition-readPosition;
    }

    @SuppressWarnings("unchecked")
    public T[] raw(){
        return (T[])buffer;
    }

    public int bufferSize() {
        return buffer.length;
    }

    @SuppressWarnings("unchecked")
    public T peek() {
        return (T)buffer[readPosition & mask];
    }

    public boolean isFull() {
        return size()==buffer.length;
    }

    public void expand(){
        buffer = Arrays.copyOf(buffer,2*buffer.length);
    }

    public void readReset() {
        readPosition = offsetReadPosition;
    }

    public void advance() {
        readPosition++;
    }

    public int readPosition() {
        return readPosition;
    }

    public void mark(){
        offsetReadPosition = readPosition;
    }
}
