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
public class ForwardingMeasuredStream<T,V extends Stats> extends AbstractMeasuredStream<T,V> {
    protected MeasuredStream<T,V> delegate;

    public ForwardingMeasuredStream(MeasuredStream<T, V> delegate) { this.delegate = delegate; }

    @Override public V getStats() { return delegate.getStats(); }
    @Override public T next() throws StreamException { return delegate.next(); }
    @Override public void close() throws StreamException { delegate.close(); }
}
