package com.splicemachine.derby.stream.spark;

import com.google.common.base.Optional;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.update.UpdateTableWriterBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

/**
 *
 *
 * @see org.apache.spark.api.java.JavaPairRDD
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
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.subtractByKey(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> f) {
        return new SparkDataSet<U>(rdd.flatMap(f));
    }

    @Override
    public <W> PairDataSet<K, V> broadcastSubtractByKey(PairDataSet<K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.subtractByKey(broadcast.value()));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterator<V>, Iterator<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.cogroup(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterator<V>, Iterator<W>>> broadcastCogroup(PairDataSet<K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Broadcast<JavaPairRDD<K,W>> broadcast = context.broadcast(rightPairDataSet);
        return new SparkPairDataSet(rdd.cogroup(broadcast.value()));
    }

    @Override
    public String toString() {
        return rdd.toString();
    }


    @Override
    public DataSet<V> insertData(InsertTableWriterBuilder builder) {
        Configuration conf = new Configuration(SIConstants.config);
        rdd.saveAsNewAPIHadoopDataset(conf);
        return null;
    }

    @Override
    public DataSet<V> updateData(UpdateTableWriterBuilder builder) {
        Configuration conf = new Configuration(SIConstants.config);
        rdd.saveAsNewAPIHadoopDataset(conf);
        return null;
    }

    @Override
    public DataSet<V> deleteData(DeleteTableWriterBuilder builder) {
        Configuration conf = new Configuration(SIConstants.config);
        rdd.saveAsNewAPIHadoopDataset(conf);
        return null;
    }
}
