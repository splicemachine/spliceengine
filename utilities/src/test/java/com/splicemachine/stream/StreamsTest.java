package com.splicemachine.stream;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StreamsTest {

    @Test
    public void of() throws StreamException {
        Stream<String> stream = Streams.of("111", "222", "333");

        assertEquals("111", stream.next());
        assertEquals("222", stream.next());
        assertEquals("333", stream.next());
    }

    @Test
    public void wrap() throws StreamException {
        List<String> list = Lists.newArrayList("111", "222", "333");

        Stream<String> stream1 = Streams.wrap(list);
        Stream<String> stream2 = Streams.wrap(list.iterator());

        assertEquals("111", stream1.next());
        assertEquals("111", stream2.next());
        assertEquals("222", stream1.next());
        assertEquals("222", stream2.next());
        assertEquals("333", stream1.next());
        assertEquals("333", stream2.next());
    }

    @Test
    public void peekingStream() throws StreamException {
        Stream<String> stream = Streams.of("111", "222", "333");

        PeekableStream<String> stream1 = Streams.peekingStream(stream);

        assertEquals("111", stream1.peek());
        assertEquals("111", stream1.next());

        assertEquals("222", stream1.peek());
        assertEquals("222", stream1.next());

        assertEquals("333", stream1.peek());
        assertEquals("333", stream1.next());

        assertEquals(null, stream1.peek());
    }
}