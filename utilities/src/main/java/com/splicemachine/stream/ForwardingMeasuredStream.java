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

import com.splicemachine.metrics.Stats;

/**
 * @author Scott Fines
 *         Date: 2/12/15
 */
public class ForwardingMeasuredStream<T,V extends Stats> extends AbstractMeasuredStream<T,V> {
    protected MeasuredStream<T,V> delegate;

    public ForwardingMeasuredStream(MeasuredStream<T, V> delegate) { this.delegate = delegate; }

    @Override public V getStats() { return delegate.getStats(); }
    @Override public T next() throws StreamException { return delegate.next(); }
    @Override public void close() throws StreamException { delegate.close(); }
}
