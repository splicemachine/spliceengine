package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.stream.function.*;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Datat Implementation using spark.
 *
 */
public class SparkDataSet<Op extends SpliceOperation,V> implements DataSet<Op,V>, Serializable {
    public JavaRDD<V> rdd;
    public SparkDataSet(JavaRDD<V> rdd) {
        this.rdd = rdd;
    }

    @Override
    public List<V> collect() {
        return rdd.collect();
    }

    @Override
    public <U> DataSet<Op,U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        return new SparkDataSet<Op,U>(rdd.mapPartitions(f));
    }

    @Override
    public DataSet<Op,V> distinct() {
        return new SparkDataSet(rdd.distinct());
    }

    @Override
    public V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2) {
        return rdd.fold(zeroValue,function2);
    }

    @Override
    public <K> PairDataSet<Op,K,V> index(SplicePairFunction<Op,K,V> function) {
        return new SparkPairDataSet(
                rdd.mapToPair(function));
    }

    @Override
    public <U> DataSet<Op,U> map(SpliceFunction<Op,V,U> function) {
        return null;
    }


    @Override
    public Iterator<V> toLocalIterator() {
        return rdd.toLocalIterator();
    }

    @Override
    public <K> PairDataSet<Op, K, V> keyBy(SpliceFunction<Op, V, K> f) {
        return new SparkPairDataSet(rdd.keyBy(f));
    }

    @Override
    public long count() {
        return rdd.count();
    }

    @Override
    public DataSet<Op, V> union(DataSet<Op, V> dataSet) {
        return new SparkDataSet<>(rdd.union(((SparkDataSet) dataSet).rdd));
    }

    @Override
    public DataSet<Op, V> filter(SplicePredicateFunction<Op, V> f) {
        return new SparkDataSet<>(rdd.filter(f));
    }

    @Override
    public DataSet<Op, V> intersect(DataSet<Op, V> dataSet) {
        return new SparkDataSet<>(rdd.intersection( ((SparkDataSet) dataSet).rdd));
    }

    @Override
    public DataSet<Op, V> subtract(DataSet<Op, V> dataSet) {
        return new SparkDataSet<>(rdd.subtract( ((SparkDataSet) dataSet).rdd));
    }
}
