package com.splicemachine.collections;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Scott Fines
 *         Date: 7/28/14
 */
public class RingBufferTest {

    @Test
    public void constructor() {
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(17);

        assertEquals(32, buffer.bufferSize());
        assertTrue(buffer.isEmpty());
        assertFalse(buffer.isFull());
        assertEquals(0, buffer.size());
        assertNull(buffer.next());
        assertNull(buffer.peek());
    }

    @Test
    public void add_next() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        List<Integer> correct = new ArrayList<Integer>();
        for (int i = 0; i < bufferSize; i++) {
            buffer.add(i);
            correct.add(i);
        }
        List<Integer> actual = new ArrayList<Integer>(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            actual.add(buffer.next());
        }

        //should have exhausted the buffer
        assertNull("Should not see another value", buffer.next());
        assertEquals("Incorrect results!", correct, actual);
    }


    @Test
    public void peek_advance() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        List<Integer> correct = new ArrayList<Integer>();
        for (int i = 0; i < bufferSize; i++) {
            buffer.add(i);
            correct.add(i);
        }
        List<Integer> actual = new ArrayList<Integer>(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            actual.add(buffer.peek());
            buffer.readAdvance();
        }

        //should have exhausted the buffer
        assertNull("Should not see another value", buffer.next());
        assertEquals("Incorrect results!", correct, actual);
    }

    @Test
    public void isFull() {
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(4);
        buffer.add(10);
        assertFalse(buffer.isFull());
        buffer.add(10);
        assertFalse(buffer.isFull());
        buffer.add(10);
        assertFalse(buffer.isFull());
        buffer.add(10);
        assertTrue(buffer.isFull());
    }

    @Test
    public void size() {
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(16);
        assertEquals(0, buffer.size());

        buffer.add(42);
        assertEquals(1, buffer.size());

        buffer.add(42);
        assertEquals(2, buffer.size());

        buffer.clear();
        assertEquals(0, buffer.size());
    }

    @Test
    public void clear() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            buffer.add(i);
        }
        buffer.clear();

        //should have exhausted the buffer
        assertEquals(0, buffer.size());
        assertFalse(buffer.isFull());
        assertTrue(buffer.isEmpty());
        assertNull("Should not see another value", buffer.next());
    }

    @Test
    public void expand() {
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(2);
        buffer.add(10);
        buffer.add(20);
        assertTrue(buffer.isFull());
        buffer.expand();
        assertFalse(buffer.isFull());
        buffer.add(30);
        buffer.add(40);
        assertTrue(buffer.isFull());

        assertEquals(4, buffer.size());
        assertEquals(4, buffer.bufferSize());

        assertEquals(10, buffer.next().intValue());
        assertEquals(20, buffer.next().intValue());
        assertEquals(30, buffer.next().intValue());
        assertEquals(40, buffer.next().intValue());

    }

    @Test
    public void mark_readReset() {
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(8);
        buffer.add(10);
        buffer.add(20);
        buffer.add(30);
        buffer.add(40);

        assertEquals(10, buffer.next().intValue());
        assertEquals(20, buffer.next().intValue());

        buffer.mark();

        assertEquals(30, buffer.next().intValue());
        assertEquals(40, buffer.next().intValue());

        buffer.readReset();

        assertEquals(30, buffer.next().intValue());
        assertEquals(40, buffer.next().intValue());
    }
}
