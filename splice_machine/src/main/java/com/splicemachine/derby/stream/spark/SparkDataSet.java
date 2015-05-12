package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Iterables;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 *
 * DataSet Implementation for Spark.
 *
 * @see com.splicemachine.derby.stream.iapi.DataSet
 * @see java.io.Serializable
 *
 */
public class SparkDataSet<V> implements DataSet<V>, Serializable {
    public JavaRDD<V> rdd;
    public int offset = -1;
    public int fetch = -1;
    public SparkDataSet(JavaRDD<V> rdd) {
        this.rdd = rdd;
    }

    public SparkDataSet(JavaRDD<V> rdd, int offset, int fetch) {
        this.offset = offset;
        this.fetch = fetch;
        this.rdd = rdd;
    }

    @Override
    public List<V> collect() {
        return rdd.collect();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        return new SparkDataSet<U>(rdd.mapPartitions(f));
    }

    @Override
    public DataSet<V> distinct() {
        return new SparkDataSet(rdd.distinct());
    }

    @Override
    public <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2) {
        return rdd.fold(zeroValue,function2);
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) {
        return new SparkPairDataSet(
                rdd.mapToPair(function));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new SparkDataSet<>(rdd.map(function));
    }


    @Override
    public Iterator<V> toLocalIterator() {
        if (offset ==-1)
            return rdd.toLocalIterator();
        return Iterables.limit(Iterables.skip(new Iterable() {
            @Override
            public Iterator iterator() {
                return rdd.toLocalIterator();
            }
        },offset), fetch).iterator();
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f) {
        return new SparkPairDataSet(rdd.keyBy(f));
    }

    @Override
    public long count() {
        return rdd.count();
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet) {
        return new SparkDataSet<>(rdd.union(((SparkDataSet) dataSet).rdd));
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new SparkDataSet<>(rdd.filter(f));
    }

    @Override
    public DataSet< V> intersect(DataSet< V> dataSet) {
        return new SparkDataSet<>(rdd.intersection( ((SparkDataSet) dataSet).rdd));
    }

    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        return new SparkDataSet<>(rdd.subtract( ((SparkDataSet) dataSet).rdd));
    }

    @Override
    public boolean isEmpty() {
        return rdd.take(1).isEmpty();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new SparkDataSet<>(rdd.flatMap(f));
    }

    @Override
    public void close() {

    }

    @Override
    public DataSet<V> fetchWithOffset(int offset, int fetch) {
        this.offset = offset;
        this.fetch = fetch;
        return this;
    }

    @Override
    public DataSet<V> take(int take) {
        JavaSparkContext ctx = SpliceSpark.getContext();
        return new SparkDataSet<V>(ctx.parallelize(rdd.take(take)));
    }
}
