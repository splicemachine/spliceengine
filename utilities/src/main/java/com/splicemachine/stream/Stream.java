/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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


import java.util.Iterator;

/**
 * Represents a Stream of values, each of which can be encountered one element at a time.
 *
 * Streams provide an alternative means of accessing data across unstable channels--that is,
 * when you wish to access data with an iterator-like interface, but you know that it may throw
 * an exception which needs to be handled, then this is a useful structure.
 *
 * However, unlike an iterator, Streams do <em>not</em> follow the {@code hasNext()/next()} paradigm.
 * Instead, they use a <em>null-stop</em> paradigm--When the stream is exhausted, it will return null.
 * This simplifies access to the stream, at the cost of making handling slightly more complex (e.g.
 * dealing with when {@code next()} returns null).
 *
 * This class extends {@link java.lang.AutoCloseable}, so it may be used within a Try-with-resources block.
 *
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface Stream<T> extends AutoCloseable{

    /**
     * @return the next element in the stream, or {@code null} if no such element exists.
     * @throws StreamException if something breaks during the next() call.
     */
    T next() throws StreamException;

    /**
     * Transform the stream in an arbitrary way. Implementations are allowed to be lazy--i.e.
     * not actually performing the transform until {@code next()} is called.
     *
     * <p>The returned Stream is also Autocloseable; closing the returned Stream should also close
     * this stream. This is to simplify close-handling.
     *
     * @param transformer the transformer to use.
     * @param <V> the new type of the returned stream.
     * @return a Stream representing the transformed stream of data.
     */
    <V> Stream<V> transform(Transformer<T, V> transformer);

    /**
     * Accumulate data using a mutable accumulator construct.
     *
     * @param accumulator the accumulator to use
     * @throws StreamException  if something breaks during the accumulation phase
     */
    void forEach(Accumulator<T> accumulator) throws StreamException;

    /**
     * Filter elements out of the stream which do not match the supplied predicate.
     *
     * The returned Stream is also AutoCloseable; closing the returned Stream should also close this stream,
     * so as to make resource-handling simpler.
     *
     * @param predicate the predicate to apply
     * @return a Stream representing the filtered stream of data.
     */
    Stream<T> filter(Predicate<T> predicate);

    /**
     * Limit the number of elements returned by the stream.
     *
     * The returned Stream is also AutoCloseable; closing the returned Stream should also close this stream,
     * so as to make resource-handling simpler.
     *
     * @param maxSize the maximum number of elements to return in the stream
     * @return a Stream representing the first {@code maxSize} elements in the stream (or less, if fewer than
     * {@code maxSize} is present in the original stream).
     */
    Stream<T> limit(long maxSize);

    /**
     * Conversion utility to allow Streams to be treated as iterators. This is useful
     * for interacting with library methods which do not allow streams.
     *
     * Be aware that the iterator interface hides exceptions by converting into runtime exceptions. While
     * this may seem appetizing to you at first, remember that we use streams specifically <em>because</em>
     * we expect certain error patterns, it is not always a good idea to hide those error patterns from view.
     *
     * @return an Iterator representation of this Stream. The returned iterator does <em>not</em> close
     * this stream, so you'll still need to explicitly close the stream.
     */
    Iterator<T> asIterator();

    /**
     * Close this Stream. This overrides the more generic {@link java.lang.AutoCloseable#close()} method
     * so as to throw a more specific exception.
     *
     * @throws StreamException if the stream cannot be closed effectively.
     */
    void close() throws StreamException;
}
