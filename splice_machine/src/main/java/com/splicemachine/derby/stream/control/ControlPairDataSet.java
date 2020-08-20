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

import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import splice.com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.direct.DirectDataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectPipelineWriter;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.kvpair.KVPair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.collect.*;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;

import static com.splicemachine.derby.stream.control.ControlUtils.limit;
import static com.splicemachine.derby.stream.control.ControlUtils.checkCancellation;
import static com.splicemachine.derby.stream.control.ControlUtils.multimapFromIterator;

/**
 *
 *
 * @see splice.com.google.common.collect.Multimap
 * @see splice.com.google.common.collect.Multimaps
 * @see splice.com.google.common.collect.Iterables
 *
 */
public class ControlPairDataSet<K,V> implements PairDataSet<K,V> {
    public Iterator<Tuple2<K,V>> source;
    public ControlPairDataSet(Iterator<Tuple2<K,V>> source) {
        this.source = source;
    }


    @Override
    public DataSet<V> values(OperationContext context) {
        return new ControlDataSet<>(Iterators.transform(source,new Function<Tuple2<K,V>, V>() {
            @Nullable @Override
            public V apply(@Nullable Tuple2<K,V>t) {
                return t==null?null:t._2();
            }
        }));
    }

    @Override
    public DataSet<V> values(String name, OperationContext context) {
        return values(context);
    }

    @Override
    public DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetails) {
        return values(context);
    }

    @Override
    public DataSet<K> keys() {
        return new ControlDataSet<>(Iterators.transform(source,new Function<Tuple2<K, V>, K>() {
            @Nullable
            @Override
            public K apply(@Nullable Tuple2<K, V> t) {
                assert t!=null;
                return t._1();
            }
        }));
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2) {
        final Iterator<Tuple2<K,V>> limitIterator = limit(checkCancellation(source,function2), function2.operationContext);
        return new ControlPairDataSet(new Iterator<Tuple2<K,V>>(){
            private Iterator<Map.Entry<K,V>> set;
            @Override
            public boolean hasNext() {
                if (set == null) {
                    try {
                        HashMap<K, V> map = Maps.newHashMap();
                        while (limitIterator.hasNext()) {
                            Tuple2<K, V> t = limitIterator.next();
                            if (map.containsKey(t._1())) {
                                function2.call( map.get(t._1()),t._2());
                            } else {
                                map.put((K)((ExecRow)t._1).getClone(), function2.call(null,t._2()));
                            }
                        }
                        set = map.entrySet().iterator();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return set.hasNext();
            }

            @Override
            public Tuple2<K,V> next() {
                Map.Entry<K,V> entry = set.next();
                return Tuple2.apply(entry.getKey(),entry.getValue());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not Implemented");
            }

            @Override
            public void forEachRemaining(Consumer action) {
                throw new UnsupportedOperationException("Not Implemented");
            }
        });
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2, boolean isLast, boolean pushScope, String scopeDetail) {
        return reduceByKey(function2);
    }
    
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(final SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new ControlDataSet<U>(Iterators.transform(checkCancellation(source,function),function));
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator, OperationContext operationContext) {
        /*
         * -sf- this is done a bit goofily, so that we can support multiple versions of guava.
         *
         * This is essentially copying out FluentIterable.from(source).toSortedList(comparator) logic
         * directly into the method, so that we can support the same code line from 12.0.1 on.
         */
        return new ControlPairDataSet<>(Ordering.from(new Comparator<Tuple2<K, V>>() {
            @Override
            public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
                return comparator.compare(o1._1(), o2._1());
            }
        }).immutableSortedCopy(() -> limit(ControlUtils.checkCancellation(source,operationContext), operationContext)).iterator());
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator, String name, OperationContext operationContext) {
        // 'name' is not used on control side
        return sortByKey(comparator, operationContext);
    }

    @Override
    public PairDataSet<K, V> partitionBy(Partitioner<K> partitioner, Comparator<K> comparator, OperationContext<JoinOperation> operationContext) {
        // we don't need to partition
        return sortByKey(comparator, operationContext);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(OperationContext context) {
        Multimap<K,V> newMap = multimapFromIterator(limit(ControlUtils.checkCancellation(source,context), context));
        return new ControlPairDataSet<>(FluentIterable.from(newMap.asMap().entrySet()).transform(new Function<Map.Entry<K, Collection<V>>, Tuple2<K, Iterable<V>>>() {
            @Nullable
            @Override
            public Tuple2<K, Iterable<V>> apply(@Nullable Map.Entry<K, Collection<V>> e) {
                assert e!=null: "E cannot be null";
                return new Tuple2<K, Iterable<V>>(e.getKey(), e.getValue());
            }
        }).iterator());
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name, OperationContext context) {
        return groupByKey(context);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet<K, W> rightDataSet, OperationContext operationContext) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterator(limit(ControlUtils.checkCancellation(((ControlPairDataSet<K,W>) rightDataSet).source,operationContext), operationContext));
        return new ControlPairDataSet(Iterators.concat(Iterators.transform(ControlUtils.checkCancellation(source,operationContext),new Function<Tuple2<K, V>, Iterator<Tuple2<K, Tuple2<V, W>>>>() {
            @Nullable
            @Override
            public Iterator<Tuple2<K, Tuple2<V, W>>> apply(@Nullable Tuple2<K, V> t) {
                assert t!=null: "Tuple cannot be null";
                List<Tuple2<K,Tuple2<V,W>>> result = new ArrayList<>();
                K key = t._1();
                V value = t._2();
                for (W rightValue : rightSide.get(key)) {
                    result.add(new Tuple2<>(key,new Tuple2<>(value,rightValue)));
                }
                return result.iterator();
            }
        })));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext) {
        // Ignore name on control side
        return hashJoin(rightDataSet, operationContext);
    }
    
    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet<K, W> rightDataSet, OperationContext operationContext) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterator(limit(ControlUtils.checkCancellation(((ControlPairDataSet<K,W>) rightDataSet).source,operationContext), operationContext));
        return new ControlPairDataSet<>(Iterators.filter(ControlUtils.checkCancellation(source,operationContext), new Predicate<Tuple2<K, V>>() {
            @Override
            public boolean apply(@Nullable Tuple2<K, V> t) {
                assert t!=null: "T cannot be null";
                return rightSide.get(t._1()).isEmpty();
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext) {
        // Ignore name on control side
        return subtractByKey(rightDataSet, operationContext);
    }

    @Override
    public String toString() {
        // We can't consume the iterator since it's a streaming iterator
        StringBuilder sb = new StringBuilder("ControlPairDataSet [");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f) {
        try {
            return new ControlDataSet<>(f.call(checkCancellation(source,f)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function) {
        try {
            Iterator<Tuple2<K, V>> source = checkCancellation(this.source,function);
            Iterator<U> iterator = Iterators.emptyIterator();
            List<Iterator<U>> l = Lists.newArrayList();

            while (source.hasNext()) {
                Tuple2<K, V> entry = source.next();
                Iterator<U> it = function.call(new Tuple2<>(entry._1(),entry._2()));
                l.add(it);
            }
            if (!l.isEmpty()) {
                iterator = Iterators.concat(l.iterator());
            }
            return new ControlDataSet<>(iterator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function, boolean isLast) {
        return flatmap(function);
    }
    
    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, OperationContext operationContext) {
        Multimap<K, V> left = multimapFromIterator(limit(ControlUtils.checkCancellation(source,operationContext), operationContext));
        Multimap<K, W> right = multimapFromIterator(limit(ControlUtils.checkCancellation(((ControlPairDataSet<K, W>) rightDataSet).source, operationContext), operationContext));

        List<Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> result = new ArrayList<>();
        for (K key: Sets.union(left.keySet(),right.keySet())){
            Collection<V> vs=left.get(key);
            Collection<W> ws=right.get(key);
            result.add(new Tuple2<>(key,new Tuple2<Iterable<V>, Iterable<W>>(vs,ws)));
        }
        return new ControlPairDataSet<>(result.iterator());
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext) {
        // Ignore name on control side
        return cogroup(rightDataSet, operationContext);
    }

    @Override
    public PairDataSet<K, V> union(PairDataSet<K, V> dataSet) {
        return new ControlPairDataSet(Iterators.concat(source,((ControlPairDataSet<K,V>)dataSet).source));
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public DataSetWriterBuilder directWriteData() throws StandardException{
        return new DirectTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null: "Txn is null";
                DirectPipelineWriter writer = new DirectPipelineWriter(destConglomerate,
                        txn, token, opCtx,skipIndex, tableVersion);

                return new DirectDataSetWriter<>((ControlPairDataSet<K,KVPair>)ControlPairDataSet.this,writer);
            }
        };
    }


}
