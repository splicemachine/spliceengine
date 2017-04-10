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

package com.splicemachine.derby.stream.control;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.delete.DeletePipelineWriter;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.direct.DirectDataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectPipelineWriter;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdatePipelineWriter;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.kvpair.KVPair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.sparkproject.guava.base.Predicate;
import org.sparkproject.guava.collect.*;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.*;
import static com.splicemachine.derby.stream.control.ControlUtils.entryToTuple;
import static com.splicemachine.derby.stream.control.ControlUtils.multimapFromIterable;

/**
 *
 *
 * @see org.sparkproject.guava.collect.Multimap
 * @see org.sparkproject.guava.collect.Multimaps
 * @see org.sparkproject.guava.collect.Iterables
 *
 */
public class ControlPairDataSet<K,V> implements PairDataSet<K,V> {
    public Iterable<Tuple2<K,V>> source;
    public ControlPairDataSet(Iterable<Tuple2<K,V>> source) {
        this.source = source;
    }



    @Override
    public DataSet<V> values() {
        return new ControlDataSet<>(FluentIterable.from(source).transform(new Function<Tuple2<K,V>, V>() {
            @Nullable @Override
            public V apply(@Nullable Tuple2<K,V>t) {
                assert t!=null;
                return t._2();
            }
        }));
    }

    @Override
    public DataSet<V> values(String name) {
        return values();
    }

    @Override
    public DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetails) {
        return values();
    }

    @Override
    public DataSet<K> keys() {
        return new ControlDataSet<>(FluentIterable.from(source).transform(new Function<Tuple2<K, V>, K>() {
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
        Multimap<K,V> newMap = multimapFromIterable(source);
        return new ControlPairDataSet<>(entryToTuple(Multimaps.forMap(Maps.transformValues(newMap.asMap(), new Function<Collection<V>, V>() {
            @Override
            public V apply(@Nullable Collection<V> vs) {
                try {
                    V returnValue = null;
                    for (V v : vs) {
                        returnValue = function2.call(returnValue, v);
                    }
                    return returnValue;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })).entries()));
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2, boolean isLast, boolean pushScope, String scopeDetail) {
        return reduceByKey(function2);
    }
    
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(final SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new ControlDataSet<U>(FluentIterable.from(source).transform(function));
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator) {
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
        }).immutableSortedCopy(source));
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator, String name) {
        // 'name' is not used on control side
        return sortByKey(comparator);
    }

    @Override
    public PairDataSet<K, V> partitionBy(Partitioner<K> partitioner, Comparator<K> comparator) {
        // we don't need to partition
        return sortByKey(comparator);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey() {
        Multimap<K,V> newMap = multimapFromIterable(source);
        return new ControlPairDataSet<>(FluentIterable.from(newMap.asMap().entrySet()).transform(new Function<Map.Entry<K, Collection<V>>, Tuple2<K, Iterable<V>>>() {
            @Nullable
            @Override
            public Tuple2<K, Iterable<V>> apply(@Nullable Map.Entry<K, Collection<V>> e) {
                assert e!=null: "E cannot be null";
                return new Tuple2<K, Iterable<V>>(e.getKey(), e.getValue());
            }
        }));
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name) {
        return groupByKey();
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(final PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet) rightDataSet).source);

        return new ControlPairDataSet<>(Iterables.concat(FluentIterable.from(source).transform(new Function<Tuple2<K, V>, Iterable<Tuple2<K, Tuple2<V, Optional<W>>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<V, Optional<W>>>> apply(@Nullable Tuple2<K, V> t) {
                assert t!=null: "T cannot be null";
                List<Tuple2<K,Tuple2<V,Optional<W>>>> result = new ArrayList<>();
                K key = t._1();
                V value = t._2();
                if (rightSide.containsKey(key)) {
                    for (W rightValue : rightSide.get(key)) {
                        result.add(new Tuple2<>(key,new Tuple2<>(value,Optional.of(rightValue))));
                    }
                } else
                    result.add(new Tuple2<>(key,new Tuple2<>(value,Optional.<W>absent())));
                return result;
            }
        })));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        // Materializes the left side
        final Multimap<K, V> leftSide = multimapFromIterable(source);


        return new ControlPairDataSet<>(Iterables.concat(FluentIterable.from(((ControlPairDataSet<K,W>) rightDataSet).source).transform(new Function<Tuple2<K, W>, Iterable<Tuple2<K, Tuple2<Optional<V>, W>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<Optional<V>, W>>> apply(@Nullable Tuple2<K, W> t) {
                assert t!=null: "t cannot be null!";
                List<Tuple2<K,Tuple2<Optional<V>,W>>> result = new ArrayList<>();
                K key = t._1();
                W value = t._2();
                if (leftSide.containsKey(key)) {
                    for (V leftValue: leftSide.get(key)) {
                        result.add(new Tuple2<>(key,new Tuple2<>(Optional.of(leftValue),value)));
                    }
                } else
                    result.add(new Tuple2<>(key,new Tuple2<>(Optional.<V>absent(),value)));
                return result;
            }
        })));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet<K,W>) rightDataSet).source);
        return new ControlPairDataSet<>(Iterables.concat(FluentIterable.from(source).transform(new Function<Tuple2<K, V>, Iterable<Tuple2<K, Tuple2<V, W>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<V, W>>> apply(@Nullable Tuple2<K, V> t) {
                assert t!=null: "Tuple cannot be null";
                List<Tuple2<K,Tuple2<V,W>>> result = new ArrayList<>();
                K key = t._1();
                V value = t._2();
                for (W rightValue : rightSide.get(key)) {
                    result.add(new Tuple2<>(key,new Tuple2<>(value,rightValue)));
                }
                return result;
            }
        })));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet, String name) {
        // Ignore name on control side
        return hashJoin(rightDataSet);
    }
    
    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet<K,W>) rightDataSet).source);
        return new ControlPairDataSet<>(FluentIterable.from(source).filter(new Predicate<Tuple2<K, V>>() {
            @Override
            public boolean apply(@Nullable Tuple2<K, V> t) {
                assert t!=null: "T cannot be null";
                return rightSide.get(t._1()).isEmpty();
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet, String name) {
        // Ignore name on control side
        return subtractByKey(rightDataSet);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ControlPairDataSet [");
        int i = 0;
        for (Tuple2<K,V> entry: source) {
            if (i++ != 0)
                sb.append(",");
            sb.append(entry);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f) {
        try {
            return new ControlDataSet<>(f.call(source.iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function) {
        try {

            List<Iterable<U>> iterables = Lists.newArrayList();
            for (Tuple2<K, V> entry : source) {
                iterables.add(function.call(new Tuple2<>(entry._1(),entry._2())));
            }
            Iterable<U> iterable = Iterables.concat(iterables);
            return new ControlDataSet<>(iterable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function, boolean isLast) {
        return flatmap(function);
    }
    
    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        Multimap<K, V> left = multimapFromIterable(source);
        Multimap<K, W> right = multimapFromIterable(((ControlPairDataSet<K, W>) rightDataSet).source);

        List<Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> result = new ArrayList<>();
        for (K key: Sets.union(left.keySet(),right.keySet())){
            Collection<V> vs=left.get(key);
            Collection<W> ws=right.get(key);
            result.add(new Tuple2<>(key,new Tuple2<Iterable<V>, Iterable<W>>(vs,ws)));
        }
        return new ControlPairDataSet<>(result);
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, String name) {
        // Ignore name on control side
        return cogroup(rightDataSet);
    }

    @Override
    public PairDataSet<K, V> union(PairDataSet<K, V> dataSet) {
        return new ControlPairDataSet<>(Iterables.concat(source,((ControlPairDataSet<K,V>)dataSet).source));
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException{
        return new DeleteTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                DeletePipelineWriter dpw = new DeletePipelineWriter(txn,heapConglom,operationContext);
                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,dpw,operationContext);
            }
        };
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new InsertTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                InsertPipelineWriter ipw = new InsertPipelineWriter(pkCols,
                        tableVersion,
                        execRowDefinition,
                        autoIncrementRowLocationArray,
                        spliceSequences,
                        heapConglom,
                        txn,
                        operationContext,
                        isUpsert);
                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,ipw,operationContext);
            }
        }.operationContext(operationContext);
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new UpdateTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null: "Txn is null";
                UpdatePipelineWriter upw =new UpdatePipelineWriter(heapConglom,
                        formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                        txn,execRowDefinition,heapList,operationContext);

                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,upw,operationContext);
            }
        };
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
                        txn, opCtx,skipIndex);

                return new DirectDataSetWriter<>((ControlPairDataSet<K,KVPair>)ControlPairDataSet.this,writer);
            }
        };
    }

}
