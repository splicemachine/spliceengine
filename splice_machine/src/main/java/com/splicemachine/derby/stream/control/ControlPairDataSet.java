package com.splicemachine.derby.stream.control;

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriter;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriter;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.update.UpdateTableWriter;
import com.splicemachine.derby.stream.temporary.update.UpdateTableWriterBuilder;
import org.sparkproject.guava.common.base.Function;
import org.sparkproject.guava.common.base.Supplier;
import org.sparkproject.guava.common.collect.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 *
 * @see org.sparkproject.guava.common.collect.Multimap
 * @see org.sparkproject.guava.common.collect.Multimaps
 * @see org.sparkproject.guava.common.collect.Iterables
 *
 */
public class ControlPairDataSet<K,V> implements PairDataSet<K,V> {
    public Multimap<K,V> source;
    public ControlPairDataSet(Multimap<K,V> source) {
        this.source = source;
    }

    @Override
    public DataSet<V> values() {
        return new ControlDataSet(source.values());
    }

    @Override
    public DataSet<K> keys() {
        return new ControlDataSet(source.keys());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2) {
        return new ControlPairDataSet<K,V>(Multimaps.forMap(Maps.transformValues(source.asMap(),new Function<Collection<V>, V>() {
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
        })));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(final SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new ControlDataSet<U>(Multimaps.transformEntries(source,new Maps.EntryTransformer<K,V,U>(){
            @Override
            public U transformEntry(@Nullable K k, @Nullable V v) {
                try {
                    return function.call(new Tuple2<K, V>(k, v));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).values());
    }

    @Override
    public PairDataSet<K, V> sortByKey(Comparator<K> comparator) {
        Multimap<K,V> newSource = Multimaps.newListMultimap(
                new TreeMap<K, Collection<V>>(comparator),
                new Supplier<List<V>>() {
                    public List<V> get() {
                        return Lists.newArrayList();
                    }
                });
        newSource.putAll(source);
        return new ControlPairDataSet<>(newSource);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey() {
        Multimap<K, Iterable<V>> multiMap = ArrayListMultimap.create();
        for (Map.Entry<K, Collection<V>> entry : source.asMap().entrySet()) {
            multiMap.put(entry.getKey(), entry.getValue());
        }
        return new ControlPairDataSet<>(multiMap);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(final PairDataSet< K, W> rightDataSet) {
        // Initialize the left side
        Multimap<K, Tuple2<V, Optional<W>>> newMap = ArrayListMultimap.<K, Tuple2<V, Optional<W>>>create();
        // Materializes the right side
        Multimap<K,W> rightSide = ArrayListMultimap.<K,W>create();
        rightSide.putAll(((ControlPairDataSet) rightDataSet).source);

        for (Map.Entry<K,V> entry : source.entries()) {
            if (rightSide.containsKey(entry.getKey())) {
                for (W rightValue: rightSide.get(entry.getKey())) {
                    newMap.put(entry.getKey(),new Tuple2<V, Optional<W>>(entry.getValue(),Optional.<W>of(rightValue)));
                }
            } else
                newMap.put(entry.getKey(),new Tuple2<V, Optional<W>>(entry.getValue(),Optional.<W>absent()));
        }
        return new ControlPairDataSet< K, Tuple2<V, Optional<W>>>(newMap);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        // Initialize the right side
        Multimap<K, Tuple2<Optional<V>, W>> newMap = ArrayListMultimap.<K, Tuple2<Optional<V>, W>>create();
        // Materializes the left side
        Multimap<K,V> leftSide = ArrayListMultimap.<K,V>create();
        leftSide.putAll(source);
        for (Map.Entry<K,W> entry : ((ControlPairDataSet<K,W>) rightDataSet).source.entries()) {
            if (leftSide.containsKey(entry.getKey())) {
                for (V leftValue: leftSide.get(entry.getKey())) {
                    newMap.put(entry.getKey(),new Tuple2<Optional<V>, W>(Optional.<V>of(leftValue),entry.getValue()));
                }
            } else
                newMap.put(entry.getKey(),new Tuple2<Optional<V>, W>(Optional.<V>absent(),entry.getValue()));
        }
        return new ControlPairDataSet< K, Tuple2<Optional<V>, W>>(newMap);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet) {
        // Initialize the left side
        Multimap<K, Tuple2<V, W>> newMap = ArrayListMultimap.<K, Tuple2<V, W>>create();
        // Materializes the right side
        Multimap<K,W> rightSide = ArrayListMultimap.<K,W>create();
        rightSide.putAll(((ControlPairDataSet) rightDataSet).source);
        for (Map.Entry<K,V> entry : source.entries()) {
            Collection<W> right = rightSide.get(entry.getKey());
            for (W w: right) {
                newMap.put(entry.getKey(),new Tuple2<V, W>(entry.getValue(),w));
            }
        }
        return new ControlPairDataSet< K, Tuple2<V, W>>(newMap);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> broadcastLeftOuterJoin(PairDataSet< K, W> rightDataSet) {
        return hashLeftOuterJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> broadcastRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        return hashRightOuterJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> broadcastJoin(PairDataSet< K, W> rightDataSet) {
        return hashJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet) {
        // Initialize the left side
        Multimap<K,V> newMap = ArrayListMultimap.<K,V>create();
        // Materializes the right side
        Multimap<K,W> rightSide = ArrayListMultimap.<K,W>create();
        rightSide.putAll(((ControlPairDataSet) rightDataSet).source);
        for (Map.Entry<K,V> entry : source.entries()) {
            Collection<W> right = rightSide.get(entry.getKey());
            if (right.size() == 0)
                newMap.put(entry.getKey(),entry.getValue());
        }
        return new ControlPairDataSet< K, V>(newMap);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer("ControlPairDataSet [");
        int i = 0;
        for (Map.Entry<K,V> entry: source.entries()) {
            if (i++ != 0)
                sb.append(",");
            sb.append(entry);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function) {
        try {
            Iterable<U> iterable = new ArrayList<U>(0);
            for (Map.Entry<K, V> entry : source.entries()) {
                iterable = Iterables.concat(iterable, function.call(new Tuple2<K, V>(entry.getKey(),entry.getValue())));
            }
            return new ControlDataSet<>(iterable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <W> PairDataSet<K, V> broadcastSubtractByKey(PairDataSet<K, W> rightDataSet) {
        return subtractByKey(rightDataSet);
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterator<V>, Iterator<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        Multimap<K, Tuple2<Iterator<V>, Iterator<W>>> newMap = ArrayListMultimap.create();
        Multimap<K,W> rightSide = ((ControlPairDataSet) rightDataSet).source;
        Sets.union(source.keySet(),rightSide.keySet());
        for (K key: Sets.union(source.keySet(),rightSide.keySet()))
            newMap.put(key, new Tuple2(source.get(key).iterator(), rightSide.get(key).iterator()));
        return new ControlPairDataSet<>(newMap);
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterator<V>, Iterator<W>>> broadcastCogroup(PairDataSet<K, W> rightDataSet) {
        return cogroup(rightDataSet);
    }

    /*
    Cleanup This code...
     */
    @Override
    public DataSet<V> insertData(InsertTableWriterBuilder builder, OperationContext operationContext) {
        InsertTableWriter insertTableWriter = null;
        try {
            insertTableWriter = builder.build();
            insertTableWriter.open();
            insertTableWriter.write((Iterator < ExecRow >) values().toLocalIterator());
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (insertTableWriter != null)
                        insertTableWriter.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public DataSet<V> updateData(UpdateTableWriterBuilder builder, OperationContext operationContext) {
        UpdateTableWriter updateTableWriter = null;
        try {
            updateTableWriter = builder.build();
            updateTableWriter.open();
            updateTableWriter.update((Iterator < ExecRow >) values().toLocalIterator());
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (updateTableWriter != null)
                    updateTableWriter.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public DataSet<V> deleteData(DeleteTableWriterBuilder builder, OperationContext operationContext) {
        DeleteTableWriter deleteTableWriter = null;
        try {
            deleteTableWriter = builder.build();
            deleteTableWriter.open();
            deleteTableWriter.delete((Iterator < ExecRow >) values().toLocalIterator());
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (deleteTableWriter != null)
                    deleteTableWriter.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


    }
}