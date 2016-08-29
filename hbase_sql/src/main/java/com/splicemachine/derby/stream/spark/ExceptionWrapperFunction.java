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

package com.splicemachine.derby.stream.spark;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;
import java.io.Serializable;
import java.util.Iterator;

/**
 * This function wraps any Exception caused by the read stack in Spark in a Tuple2, for consumption by the
 * RecordWriter implementation.
 */
public class ExceptionWrapperFunction<K,V> implements PairFlatMapFunction<Iterator<Tuple2<K,V>>, K,Either<Exception,V>>, Serializable {
    @Override
    public Iterator<Tuple2<K, Either<Exception, V>>> call(final Iterator<Tuple2<K, V>> tuple2Iterator) throws Exception {
            return new IteratorExceptionWrapper(tuple2Iterator);
    };
}

class IteratorExceptionWrapper<K,V> implements Iterator<Tuple2<K, ? extends Either<Exception, V>>> {
    Iterator<Tuple2<K, V>> delegate;
    Exception caught;
    boolean consumed = false;

    public IteratorExceptionWrapper(Iterator<Tuple2<K, V>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (consumed)
            return false;
        try {
            return caught != null || delegate.hasNext();
        } catch (Exception e) {
            caught = e;
            return true;
        }
    }

    @Override
    public Tuple2<K, ? extends Either<Exception, V>> next() {
        if (caught != null) {
            consumed = true;
            return new Tuple2<>(null, new Left<Exception, V>(caught));
        }
        try {
            Tuple2<K, V> result = delegate.next();
            return new Tuple2<>(result._1(), new Right<Exception, V>(result._2()));
        } catch (Exception e) {
            consumed = true;
            return new Tuple2<>(null, new Left<Exception, V>(e));
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented");
    }
}