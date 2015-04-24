package com.splicemachine.derby.stream.control;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import org.sparkproject.guava.common.collect.Iterables;
import org.sparkproject.guava.common.collect.Multimaps;
import org.sparkproject.guava.common.collect.Sets;
import com.splicemachine.derby.stream.*;
import org.sparkproject.guava.common.collect.FluentIterable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSet<Op extends SpliceOperation,V> implements DataSet<Op,V> {
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
    public <U> DataSet<Op,U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        try {
            return new ControlDataSet<>(f.call(FluentIterable.from(iterable).iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<Op,V> distinct() {
        return new ControlDataSet(Sets.newHashSet(iterable));
    }

    @Override
    public V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2) {
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
    public <K>PairDataSet<Op,K, V> index(SplicePairFunction<Op,K,V> function) {
        return new ControlPairDataSet<Op,K,V>(Multimaps.index(iterable,function));
    }

    @Override
    public <U> DataSet<Op,U> map(SpliceFunction<Op,V,U> function) {
        return new ControlDataSet<Op,U>(Iterables.transform(iterable,function));
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return iterable.iterator();
    }


    @Override
    public <K> PairDataSet<Op, K, V> keyBy(SpliceFunction<Op, V, K> function) {

        return new ControlPairDataSet<Op,K,V>(Multimaps.index(iterable,function));
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
    public DataSet<Op, V> union(DataSet<Op, V> dataSet) {
        return new ControlDataSet(Iterables.concat(iterable, ((ControlDataSet) dataSet).iterable));
    }

    @Override
    public DataSet<Op, V> filter(SplicePredicateFunction<Op, V> f) {
        return new ControlDataSet<>(Iterables.filter(iterable,f));
    }

    @Override
    public DataSet<Op, V> intersect(DataSet<Op, V> dataSet) {
        return new ControlDataSet<>(Sets.intersection(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
    }

    @Override
    public DataSet<Op, V> subtract(DataSet<Op, V> dataSet) {
        return new ControlDataSet<>(Sets.difference(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
    }


}