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
public class SparkPairDataSet<Op extends SpliceOperation,K,V> implements PairDataSet<Op,K,V> {
    public JavaPairRDD<K,V> rdd;
    public SparkPairDataSet(JavaPairRDD<K,V> rdd) {
        this.rdd = rdd;
    }

    @Override
    public DataSet<Op,V> values() {
        return new SparkDataSet<Op,V>(rdd.values());
    }

    @Override
    public DataSet<Op,K> keys() {
        return new SparkDataSet<Op,K>(rdd.keys());
    }

    @Override
    public PairDataSet<Op,K, V> reduceByKey(SpliceFunction2<Op,V, V, V> function2) {
        return new SparkPairDataSet<>(rdd.reduceByKey(function2));
    }

    @Override
    public <U> DataSet<Op,U> map(SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new SparkDataSet<Op,U>(rdd.map(function));
    }

    @Override
    public PairDataSet<Op, K, V> sortByKey(Comparator<K> comparator) {
        return new SparkPairDataSet<>(rdd.sortByKey(comparator));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.leftOuterJoin( ((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.rightOuterJoin(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, W>> hashJoin(PairDataSet<Op, K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.join(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, Optional<W>>> broadcastLeftOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.leftOuterJoin(broadcast.value()));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<Optional<V>, W>> broadcastRightOuterJoin(PairDataSet<Op, K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.rightOuterJoin(broadcast.value()));
    }

    @Override
    public <W> PairDataSet<Op, K, Tuple2<V, W>> broadcastJoin(PairDataSet<Op, K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.join(broadcast.value()));
    }

    @Override
    public <W> PairDataSet<Op, K, V> subtract(PairDataSet<Op, K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.subtract( ((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public String toString() {
        return rdd.toString();
    }


}
