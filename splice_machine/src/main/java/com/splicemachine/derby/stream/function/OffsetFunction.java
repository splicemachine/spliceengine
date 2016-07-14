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

package com.splicemachine.derby.stream.function;

import com.google.common.base.Function;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.sparkproject.guava.collect.Iterables;
import org.sparkproject.guava.collect.Iterators;
import org.sparkproject.guava.collect.PeekingIterator;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 11/3/15.
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
    public Iterable<V> call(Iterator<Tuple2<V, Long>> in) throws Exception {
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
            });
        }
        // consumed
        return Collections.emptyList();
    }
}