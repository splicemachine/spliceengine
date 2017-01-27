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
 *         Date: 12/22/14
 */
public abstract class AbstractMeasuredStream<T,V extends Stats> extends AbstractStream<T> implements MeasuredStream<T,V>  {

    @Override
    public <K> MeasuredStream<K,V> transform(Transformer<T, K> transformer) {
        return new MeasuredStreams.TransformingMeasuredStream<>(this,transformer);
    }

    @Override
    public MeasuredStream<T,V> filter(Predicate<T> predicate) {
        return new MeasuredStreams.FilteredMeasuredStream<>(this,predicate);
    }

    @Override
    public MeasuredStream<T,V> limit(long maxSize) {
        return new MeasuredStreams.LimitedStream<>(this,maxSize);
    }
}
