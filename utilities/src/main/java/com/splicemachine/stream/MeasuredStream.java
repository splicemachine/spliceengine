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
 *         Date: 12/19/14
 */
public interface MeasuredStream<T,V extends Stats> extends Stream<T> {

    /**
     * @return stats collected <em>as of this point in time</em>. If the stream has not been
     * exhausted, then calling {@code next()} may change the value of the returned stats. This change
     * may or may not be reflected in the stats returned, depending on the implementation.
     */
    V getStats();

    @Override
    <R> MeasuredStream<R,V> transform(Transformer<T, R> transformer);

    @Override
    MeasuredStream<T,V> filter(Predicate<T> predicate);

    @Override
    MeasuredStream<T,V> limit(long maxSize);
}
