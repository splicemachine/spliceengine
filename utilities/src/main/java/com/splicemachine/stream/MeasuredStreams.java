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

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 2/12/15
 */
public class MeasuredStreams {


    static class TransformingMeasuredStream<E,R,V extends Stats> extends AbstractMeasuredStream<R,V>{
        private final MeasuredStream<E,V> delegate;
        private final Transformer<E, R> transformer;

        public TransformingMeasuredStream(MeasuredStream<E, V> delegate, Transformer<E, R> transformer) {
            this.delegate = delegate;
            this.transformer = transformer;
        }

        @Override
        public <K> MeasuredStream<K,V> transform(Transformer<R, K> transformer) {
            return new TransformingMeasuredStream<>(this,transformer);
        }

        @Override
        public R next() throws StreamException {
            E n = delegate.next();
            if(n==null) return null;
            return transformer.transform(n);
        }

        @Override public void close() throws StreamException { delegate.close(); }

        @Override
        public V getStats() {
            return delegate.getStats();
        }
    }

    static final class FilteredMeasuredStream<T,V extends Stats> extends ForwardingMeasuredStream<T,V>{
        private final Predicate<T> predicate;

        public FilteredMeasuredStream(MeasuredStream<T,V> delegate, Predicate<T> predicate) {
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

    static final class LimitedStream<T,V extends Stats> extends ForwardingMeasuredStream<T,V> {
        private final long maxSize;
        private long numReturned;

        public LimitedStream(MeasuredStream<T,V> stream, long maxSize) {
            super(stream);
            this.maxSize = maxSize;
        }

        @Override
        public T next() throws StreamException {
            if(numReturned>=maxSize) return null;
            T n = delegate.next();
            if(n==null)
                numReturned = maxSize+1; //prevent extraneous calls to the underlying stream
            numReturned++;
            return n;
        }
    }

}
