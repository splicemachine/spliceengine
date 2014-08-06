package com.splicemachine.collections;

import com.splicemachine.collections.RingBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 7/28/14
 */
public class RingBufferTest {

    @Test
    public void testWorks() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        List<Integer> correct = new ArrayList<Integer>();
        for(int i=0;i< bufferSize;i++){
            buffer.add(i);
            correct.add(i);
        }
        List<Integer> actual = new ArrayList<Integer>(bufferSize);
        for(int i=0;i<bufferSize;i++){
            actual.add(buffer.next());
        }

        //should have exhausted the buffer
        Assert.assertNull("Should not see another value", buffer.next());
        Assert.assertEquals("Incorrect results!",correct,actual);
    }


    @Test
    public void testPeekAndAdvance() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        List<Integer> correct = new ArrayList<Integer>();
        for(int i=0;i< bufferSize;i++){
            buffer.add(i);
            correct.add(i);
        }
        List<Integer> actual = new ArrayList<Integer>(bufferSize);
        for(int i=0;i<bufferSize;i++){
            actual.add(buffer.peek());
            buffer.readAdvance();
        }

        //should have exhausted the buffer
        Assert.assertNull("Should not see another value", buffer.next());
        Assert.assertEquals("Incorrect results!",correct,actual);
    }

    @Test
    public void testClearWorks() throws Exception {
        int bufferSize = 10;
        RingBuffer<Integer> buffer = new RingBuffer<Integer>(bufferSize);
        for(int i=0;i< bufferSize;i++){
            buffer.add(i);
        }
        buffer.clear();

        //should have exhausted the buffer
        Assert.assertNull("Should not see another value", buffer.next());
    }
}
