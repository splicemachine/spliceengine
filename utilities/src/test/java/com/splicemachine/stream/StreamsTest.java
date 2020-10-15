/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
