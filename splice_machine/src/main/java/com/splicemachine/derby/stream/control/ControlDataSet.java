package com.splicemachine.derby.stream.control;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import org.sparkproject.guava.common.base.Function;
import org.sparkproject.guava.common.collect.Iterables;
import org.sparkproject.guava.common.collect.Multimaps;
import org.sparkproject.guava.common.collect.Sets;
import org.sparkproject.guava.common.collect.FluentIterable;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSet<V> implements DataSet<V> {
    protected Iterable<V> iterable;
    public ControlDataSet(Iterable<V> iterable) {
        this.iterable = iterable;
    }

    @Override
    public List<V> collect() {
        List<V> rows = new ArrayList<V>();
        Iterator<V> it = iterable.iterator();
        while (it.hasNext())
            rows.add(it.next());
        return rows;
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        try {
            return new ControlDataSet<>(f.call(FluentIterable.from(iterable).iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> distinct() {
        return new ControlDataSet(Sets.newHashSet(iterable));
    }

    @Override
    public <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2) {
        try {
            for (V v : iterable) {
                zeroValue = function2.call(zeroValue, v);
            }
            return zeroValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function) {
        return new ControlPairDataSet<K,U>(Multimaps.transformValues(Multimaps.index(iterable, new Function<V, K>() {
            @Nullable
            @Override
            public K apply(@Nullable V v) {
                return function.genKey(v);
            }
        }),new Function<V, U>() {
            @Nullable
            @Override
            public U apply(@Nullable V v) {
                return function.genValue(v);
            }
        }));

    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new ControlDataSet<U>(Iterables.transform(iterable,function));
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return iterable.iterator();
    }


    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> function) {

        return new ControlPairDataSet<K,V>(Multimaps.index(iterable,function));
    }

    @Override
    public String toString() {
        StringBuffer controlDataSet = new StringBuffer("ControlDataSet {");
        controlDataSet.append("}");
        return controlDataSet.toString();
    }

    @Override
    public long count() {
        return Iterables.size(iterable);
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet) {
        return new ControlDataSet(Iterables.concat(iterable, ((ControlDataSet) dataSet).iterable));
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new ControlDataSet<>(Iterables.filter(iterable,f));
    }

    @Override
    public DataSet< V> intersect(DataSet< V> dataSet) {
        return new ControlDataSet<>(Sets.intersection(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
    }

    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        return new ControlDataSet<>(Sets.difference(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
    }

    @Override
    public boolean isEmpty() {
        return Iterables.isEmpty(iterable);
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new ControlDataSet(FluentIterable.from(iterable).transformAndConcat(f));
    }

    @Override
    public void close() {

    }

    @Override
    public DataSet<V> fetchWithOffset(int offset, int fetch) {
        return new ControlDataSet<>(Iterables.limit(Iterables.skip(iterable,offset),fetch));
    }

    @Override
    public DataSet<V> take(int take) {
        return new ControlDataSet<V>(Iterables.limit(iterable,take));
    }

    @Override
    public void writeToDisk(String path) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(path);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (V value : iterable) {
            out.write(value.toString());
        }
        out.close();
    }
}
