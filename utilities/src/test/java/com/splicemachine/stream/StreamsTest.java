/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stream;

import org.spark_project.guava.collect.Lists;
import org.junit.Test;

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