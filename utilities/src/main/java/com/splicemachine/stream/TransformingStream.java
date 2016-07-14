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

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
class TransformingStream<T,V> extends AbstractStream<V> {
    private final Stream<T> stream;
    private final Transformer<T, V> transformer;

    TransformingStream(Stream<T> stream, Transformer<T, V> transformer) {
        this.stream = stream;
        this.transformer = transformer;
    }

    @Override
    public V next() throws StreamException {
        T n = stream.next();
        if(n==null) return null;
        return transformer.transform(n);
    }

    @Override
    public void close() throws StreamException  {stream.close(); }
}
