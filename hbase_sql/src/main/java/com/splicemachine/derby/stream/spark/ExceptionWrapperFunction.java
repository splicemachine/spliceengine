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