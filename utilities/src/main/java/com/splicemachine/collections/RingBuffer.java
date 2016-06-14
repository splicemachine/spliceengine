package com.splicemachine.collections;

import java.util.Arrays;

/**
 * Simple non-thread-safe ring buffer.
 *
 * If you need a thread-safe ring buffer, use the LMAX disruptor instead.
 *
 * @author Scott Fines
 *         Date: 7/22/14
 */
public class RingBuffer<T> {
    private int mask;
    private Object[] buffer;
    private int writePosition;
    private int readPosition;

    private int offsetReadPosition; //offset for readReset()

    public RingBuffer(int bufferSize) {
        int s = 1;
        while (s < bufferSize)
            s <<= 1;
        this.buffer = new Object[s];
        this.mask = s - 1;
        writePosition = 0;
        readPosition = 0;
    }

    /**
     * @return the next item in the buffer, or {@code null} if there are
     * no items in the buffer
     */
    public T next() {
        T n = peek();
        readPosition++;
        return n;
    }

    public void add(T element) {
        int pos = writePosition & mask;
        buffer[pos] = element;
        writePosition++;
    }

    public int size() {
        return writePosition - readPosition;
    }

    public int bufferSize() {
        return buffer.length;
    }

    @SuppressWarnings("unchecked")
    public T peek() {
        if(readPosition>=writePosition) return null; //buffer has already been fully read
        return (T) buffer[readPosition & mask];
    }

    public boolean isFull() {
        return size() == buffer.length;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void expand() {
        buffer = Arrays.copyOf(buffer, 2 * buffer.length);
        mask = buffer.length - 1;
    }

    public void readReset() {
        readPosition = offsetReadPosition;
    }

    public void readAdvance() {
        readPosition++;
    }

    public void clear() {
        //skip the read position to the write position, so that you never see any of the old data
        readPosition = writePosition;
        offsetReadPosition = readPosition;
    }

    public void mark() {
        offsetReadPosition = readPosition;
    }
}
