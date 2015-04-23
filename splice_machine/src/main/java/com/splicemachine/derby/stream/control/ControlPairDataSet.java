package com.splicemachine.derby.stream.control;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import org.sparkproject.guava.common.base.Function;
import org.sparkproject.guava.common.base.Supplier;
import org.sparkproject.guava.common.collect.*;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.PairDataSet;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlPairDataSet<Op extends SpliceOperation,K,V> implements PairDataSet<Op,K,V> {
    public Multimap<K,V> source;
    public ControlPairDataSet(Multimap<K,V> source) {
        this.source = source;
    }

    @Override
    public DataSet<Op,V> values() {
        return new ControlDataSet(source.values());
    }

    @Override
    public DataSet<Op,K> keys() {
        return new ControlDataSet(source.keys());
    }

    @Override
    public PairDataSet<Op,K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2) {
        return new ControlPairDataSet<Op,K,V>(Multimaps.forMap(Maps.transformValues(source.asMap(),new Function<Collection<V>, V>() {
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
    public <U> DataSet<Op,U> map(final SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new ControlDataSet<Op,U>(Multimaps.transformEntries(source,new Maps.EntryTransformer<K,V,U>(){
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
    public PairDataSet<Op, K, V> sortByKey(Comparator<K> comparator) {
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
    public <W> PairDataSet<Op, K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(final PairDataSet<Op, K, W> rightDataSet) {
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
        return new ControlPairDataSet<Op, K, Tuple2<V, Optional<W>>>(newMap);
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        // Initialize the right side
        Multimap<K, Tuple2<Optional<V>, W>> newMap = ArrayListMultimap.<K, Tuple2<Optional<V>, W>>create();
        // Materializes the left side
        Multimap<K,V> leftSide = ArrayListMultimap.<K,V>create();
        leftSide.putAll(source);
        for (Map.Entry<K,W> entry : ((ControlPairDataSet<Op,K,W>) rightDataSet).source.entries()) {
            if (leftSide.containsKey(entry.getKey())) {
                for (V leftValue: leftSide.get(entry.getKey())) {
                    newMap.put(entry.getKey(),new Tuple2<Optional<V>, W>(Optional.<V>of(leftValue),entry.getValue()));
                }
            } else
                newMap.put(entry.getKey(),new Tuple2<Optional<V>, W>(Optional.<V>absent(),entry.getValue()));
        }
        return new ControlPairDataSet<Op, K, Tuple2<Optional<V>, W>>(newMap);
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, W>> hashJoin(PairDataSet<Op, K, W> rightDataSet) {
        // Initialize the left side
        Multimap<K, Tuple2<V, W>> newMap = ArrayListMultimap.<K, Tuple2<V, W>>create();
        // Materializes the right side
        Multimap<K,W> rightSide = ArrayListMultimap.<K,W>create();
        rightSide.putAll(((ControlPairDataSet) rightDataSet).source);
        for (Map.Entry<K,V> entry : source.entries()) {
            Collection<W> right = rightSide.get(entry.getKey());
            if (right.size() > 0)
                newMap.put(entry.getKey(),new Tuple2<V, W>(entry.getValue(),right.iterator().next()));
        }
        return new ControlPairDataSet<Op, K, Tuple2<V, W>>(newMap);
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, Optional<W>>> broadcastLeftOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        return hashLeftOuterJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<Optional<V>, W>> broadcastRightOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        return hashRightOuterJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, W>> broadcastJoin(PairDataSet<Op, K, W> rightDataSet) {
        return hashJoin(rightDataSet);
    }

    @Override
    public <W> PairDataSet<Op, K, V> subtract(PairDataSet<Op, K, W> rightDataSet) {
 /*       // Initialize the left side
        Multimap<K, Tuple2<V, W>> newMap = ArrayListMultimap.<K, Tuple2<V, W>>create();
        // Materializes the right side
        Multimap<K,W> rightSide = ArrayListMultimap.<K,W>create();
        rightSide.putAll(((ControlPairDataSet) rightDataSet).source);

        for (Map.Entry<K,V> entry : source.entries()) {
            for (W rightValue: rightSide.get(entry.getKey())) {
                newMap.put(entry.getKey(),new Tuple2<V, W>(entry.getValue(),rightValue));
            }
        }
        return new ControlPairDataSet<Op, K, Tuple2<V, W>>(newMap);
        */
        throw new RuntimeException("Not Implemented");
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


}