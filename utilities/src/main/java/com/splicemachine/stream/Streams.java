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

import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Stats;

import java.util.Iterator;

/**
 * Utility classes around Streams.
 *
 * @author Scott Fines
 * Date: 8/13/14
 */
public class Streams {

    @SafeVarargs public static <T> Stream<T> of(T...elements){ return new ArrayStream<>(elements); }
    public static <T> Stream<T> wrap(Iterator<T> iterator){ return new IteratorStream<>(iterator); }
    public static <T> Stream<T> wrap(Iterable<T> iterable){ return new IteratorStream<>(iterable.iterator()); }
    public static <T> PeekableStream<T> peekingStream(Stream<T> stream){ return new PeekingStream<>(stream); }

    @SuppressWarnings("unchecked") public static <T> Stream<T> empty(){ return (Stream<T>)EMPTY; }

    @SuppressWarnings("unchecked") public static <T,V extends Stats> MeasuredStream<T,V> measuredEmpty(){ return (MeasuredStream<T,V>)MEASURED_EMPTY; }

    /******************************************************************************************************************/
    /*private helper methods and classes*/

    private static final AbstractStream EMPTY =  new AbstractStream() {
        @Override public Object next() throws StreamException { return null; }
        @Override public void close() throws StreamException {  }
    };

    private static final AbstractMeasuredStream MEASURED_EMPTY = new AbstractMeasuredStream() {
        @Override public Stats getStats() { return Metrics.noOpIOStats(); }
        @Override public Object next() throws StreamException { return null; }
        @Override public void close() throws StreamException {  }
    };

    static final class FilteredStream<T> extends ForwardingStream<T>{
        private final Predicate<T> predicate;

        public FilteredStream(Stream<T> delegate,Predicate<T> predicate) {
            super(delegate);
            this.predicate = predicate;
        }

        @Override
        public T next() throws StreamException {
            T n;
            while((n = delegate.next())!=null){
                if(predicate.apply(n)) return n;
            }
            return null;
        }
    }

    static final class LimitedStream<T> extends ForwardingStream<T> {
        private final long maxSize;
        private long numReturned;

        public LimitedStream(Stream<T> stream, long maxSize) {
            super(stream);
            this.maxSize = maxSize;
        }

        @Override
        public T next() throws StreamException {
            if(numReturned>maxSize) return null;
            T n = delegate.next();
            if(n==null)
                numReturned = maxSize+1; //prevent extraneous calls to the underlying stream
            return n;
        }
    }

    private static class IteratorStream<T> extends AbstractStream<T> {
        /*
         * Stream representation of an Iterator
         */
        private final Iterator<T> iterator;

        private IteratorStream(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public T next() throws StreamException {
            if(!iterator.hasNext()) return null;
            return iterator.next();
        }

        @Override public void close() throws StreamException { }//no-op
    }

    private static class ArrayStream<T> extends AbstractStream<T> {
        /*
         * Stream representation of a fixed array
         */
        private final T[] stream;
        private int position = 0;

        private ArrayStream(T[] stream) {
            this.stream = stream;
        }

        @Override
        public T next() throws StreamException {
            if(position>=stream.length) return null;
            T n =  stream[position];
            position++;
            return n;
        }

        @Override public void close() throws StreamException {  } //no-op
    }

    private static class PeekingStream<T> extends ForwardingStream<T> implements PeekableStream<T> {
        /*
         * Peekable version of any stream
         */
        private T n;

        public PeekingStream(Stream<T> stream) {
            super(stream);
        }

        @Override
        public T peek() throws StreamException {
            if(n!=null) return n;
            n = delegate.next();
            return n;
        }

        @Override
        public void take() {
            assert n!=null: "Called take without first calling peek!";
            n = null; //strip away n;
        }

        @Override
        public T next() throws StreamException {
            T next = peek();
            n = null; //strip n away
            return next;
        }
    }

}
