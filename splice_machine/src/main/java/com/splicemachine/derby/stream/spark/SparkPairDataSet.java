package com.splicemachine.derby.stream.spark;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.PairDataSet;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Created by jleach on 4/13/15.
 */
public class SparkPairDataSet<K,V> implements PairDataSet<K,V> {
    public JavaPairRDD<K,V> rdd;
    public SparkPairDataSet(JavaPairRDD<K,V> rdd) {
        this.rdd = rdd;
    }

    @Override
    public DataSet<V> values() {
        return new SparkDataSet<V>(rdd.values());
    }

    @Override
    public DataSet<K> keys() {
        return new SparkDataSet<K>(rdd.keys());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(SpliceFunction2<Op,V, V, V> function2) {
        return new SparkPairDataSet<>(rdd.reduceByKey(function2));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new SparkDataSet<U>(rdd.map(function));
    }

    @Override
    public PairDataSet< K, V> sortByKey(Comparator<K> comparator) {
        return new SparkPairDataSet<>(rdd.sortByKey(comparator));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.leftOuterJoin( ((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.rightOuterJoin(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.join(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> broadcastLeftOuterJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.leftOuterJoin(broadcast.value()));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> broadcastRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.rightOuterJoin(broadcast.value()));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> broadcastJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.join(broadcast.value()));
    }

    @Override
    public <W> PairDataSet< K, V> subtract(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.subtract( ((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public String toString() {
        return rdd.toString();
    }


}
