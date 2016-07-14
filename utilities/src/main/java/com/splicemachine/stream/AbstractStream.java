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
