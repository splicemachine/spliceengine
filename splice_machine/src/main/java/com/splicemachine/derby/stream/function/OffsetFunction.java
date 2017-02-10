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

package com.splicemachine.derby.stream.function;

import org.spark_project.guava.base.Function;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.PeekingIterator;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 *
 */
public class OffsetFunction<Op extends SpliceOperation,V> extends SpliceFlatMapFunction<Op, Iterator<Tuple2<V, Long>>, V> {
    private long limit;
    private long offset;
    public OffsetFunction() {
        super();
    }

    public OffsetFunction(OperationContext<Op> operationContext, long offset, long limit) {
        super(operationContext);
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(offset);
        out.writeLong(limit);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        offset = in.readLong();
        limit = in.readLong();
    }

    @Override
    public Iterator<V> call(Iterator<Tuple2<V, Long>> in) throws Exception {
        final PeekingIterator<Tuple2<V, Long>> peeking = Iterators.peekingIterator(in);
        while(peeking.hasNext()) {
            Tuple2<V, Long> tuple = peeking.peek();
            long index = tuple._2();
            if (index < offset) {
                peeking.next();
                continue; //skip until index >= offset
            }

            // create iterator with offset applied
            Iterable<Tuple2<V, Long>> result = new Iterable<Tuple2<V, Long>>() {
                @Override
                public Iterator<Tuple2<V, Long>> iterator() {
                    return peeking;
                }
            };
            // if limit, apply it
            if (limit > 0) {
                result = Iterables.limit(result, (int) (offset - index + limit));
            }
            // take only the values
            return Iterables.transform(result, new Function<Tuple2<V, Long>, V>() {
                @Nullable
                @Override
                public V apply(@Nullable Tuple2<V, Long> tuple) {
                    return tuple._1();
                }
            }).iterator();
        }
        // consumed
        return Collections.<V>emptyList().iterator();
    }
}