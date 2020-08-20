/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.*;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CancellationException;


/**
 * Created by dgomezferro on 7/31/15.
 */
public class ControlUtils {
    public static <K, V> Iterator<Tuple2<K, V>> entryToTuple(Collection<Map.Entry<K, V>> collection) {
        return Iterators.transform(collection.iterator(),new Function<Map.Entry<K, V>, Tuple2<K, V>>() {

            @Nullable
            @Override
            public Tuple2<K, V> apply(@Nullable Map.Entry<K, V> kvEntry) {
                assert kvEntry!=null;
                return new Tuple2<>(kvEntry.getKey(),kvEntry.getValue());
            }
        });
    }

    public static <K, V> Multimap<K,V> multimapFromIterator(Iterator<Tuple2<K, V>> iterator) {
        Multimap<K,V> newMap = ArrayListMultimap.create();
        while (iterator.hasNext()) {
            Tuple2<K, V> t = iterator.next();
            newMap.put(t._1(), t._2());
        }
        return newMap;
    }

    public static <E> Iterator<E> limit(Iterator<E> delegate, OperationContext context) {
        if (context == null) {
            // no context, iterator is unlimited
            return delegate;
        }
        ControlExecutionLimiter limiter = context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter();
        return new LimitedIterator<E>(delegate, limiter);
    }


    private static class LimitedIterator<E> implements Iterator<E> {
        private final Iterator<E> delegate;
        private final ControlExecutionLimiter limiter;

        public LimitedIterator(Iterator<E> delegate, ControlExecutionLimiter limiter) {
            this.delegate = delegate;
            this.limiter = limiter;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public E next() {
            limiter.addAccumulatedRows(1);
            return delegate.next();
        }
    }

    public static <E> Iterator<E> checkCancellation(Iterator<E> iterator, AbstractSpliceFunction f) {
        return checkCancellation(iterator, f.operationContext);
    }

    public static <E> Iterator<E> checkCancellation(Iterator<E> iterator, OperationContext<?> opContext) {
        if (opContext == null)
            return iterator;
        StatementContext context = opContext.getActivation().getLanguageConnectionContext().getStatementContext();
        return Iterators.transform(iterator, new Function<E, E>() {
            @Nullable
            @Override
            public E apply(@Nullable E v) {
                if (context.isCancelled())
                    throw new CancellationException("Operation was cancelled");
                return v;
            }
        });
    }

}
