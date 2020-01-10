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

import java.util.Iterator;

/**
 * Skeleton implementation of the Stream interface
 * @author Scott Fines
 * Date: 8/13/14
 */
public abstract class AbstractStream<T> implements Stream<T> {

    @Override
    public <V> Stream<V> transform(Transformer<T, V> transformer) {
        return new TransformingStream<>(this,transformer);
    }

    @Override
    public void forEach(Accumulator<T> accumulator) throws StreamException {
        T n;
        while((n = next())!=null){
            accumulator.accumulate(n);
        }
    }

    @Override public Stream<T> filter(Predicate<T> predicate) { return new Streams.FilteredStream<>(this,predicate); }
    @Override public Iterator<T> asIterator() { return new StreamIterator<>(this); }
    @Override public Stream<T> limit(long maxSize) { return new Streams.LimitedStream<>(this,maxSize);}
}
