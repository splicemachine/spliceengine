package com.splicemachine.derby.stream.spark;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * This function wraps any Exception caused by the read stack in Spark in a Tuple2, for consumption by the
 * RecordWriter implementation.
 */
public class ExceptionWrapperFunction<K> implements PairFlatMapFunction<Iterator<Tuple2<K,Object>>, K,Object>, Serializable {
    @Override
    public Iterable<Tuple2<K, Object>> call(final Iterator<Tuple2<K, Object>> tuple2Iterator) throws Exception {
        return new Iterable<Tuple2<K, Object>>() {
            @Override
            public Iterator<Tuple2<K, Object>> iterator() {
                return new IteratorExceptionWrapper(tuple2Iterator);
            }
        };
    }
}

class IteratorExceptionWrapper<K> implements Iterator<Tuple2<K, Object>> {
    Iterator<Tuple2<K, Object>> delegate;
    Exception caught;
    boolean consumed = false;

    public IteratorExceptionWrapper(Iterator<Tuple2<K, Object>> delegate) {
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
    public Tuple2<K, Object> next() {
        if (caught != null) {
            consumed = true;
            return new Tuple2<>(null, (Object) caught);
        }
        try {
            return delegate.next();
        } catch (Exception e) {
            consumed = true;
            return new Tuple2<>(null, (Object) e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented");
    }
}