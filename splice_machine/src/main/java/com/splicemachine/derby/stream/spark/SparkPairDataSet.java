package com.splicemachine.derby.stream.spark;

import com.google.common.base.Optional;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.SMOutputFormat;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.update.UpdateTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import scala.Tuple2;
import java.util.*;

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
    public PairDataSet<K, Iterable<V>> groupByKey() {
        return new SparkPairDataSet<>(rdd.groupByKey());
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.leftOuterJoin(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.rightOuterJoin(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.join(((SparkPairDataSet) rightDataSet).rdd));
    }

    private <W> Multimap<K,W> generateMultimap(JavaPairRDD<K,W> rightPairDataSet) {
        Multimap<K,W> returnValue = ArrayListMultimap.create();
        List<Tuple2<K,W>> value = rightPairDataSet.collect();
        for (Tuple2<K,W> tuple: value) {
            returnValue.put(tuple._1,tuple._2);
        }
        return returnValue;
    }

    private <W> Set<K> generateKeySet (JavaPairRDD<K,W> rightPairDataSet) {
        return rightPairDataSet.collectAsMap().keySet();
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> broadcastLeftOuterJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Multimap<K,W> rightBroadcast = generateMultimap(rightPairDataSet);
        final Broadcast<Multimap<K,W>> broadcast = context.broadcast(rightBroadcast);
        return new SparkPairDataSet(rdd.flatMapToPair(new LeftOuterJoinPairFlatMapFunction(broadcast)));
    }


    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> broadcastRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Multimap<K,V> leftDataSet = generateMultimap(rdd);
        Broadcast<Multimap<K,V>> broadcast = context.broadcast(leftDataSet);
        return new SparkPairDataSet(rightPairDataSet.flatMapToPair(new RightOuterJoinPairFlatMapFunction(broadcast)));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> broadcastJoin(PairDataSet< K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Multimap<K, W> rightBroadcast = generateMultimap(rightPairDataSet);
        final Broadcast<Multimap<K, W>> broadcast = context.broadcast(rightBroadcast);
        return new SparkPairDataSet(rdd.flatMapToPair(new JoinPairFlatMapFunction(broadcast)));
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
        Set<K> keyBroadcast = generateKeySet(rightPairDataSet);
        Broadcast<Set<K>> broadcast = context.broadcast(keyBroadcast);
        return new SparkPairDataSet(rdd.flatMapToPair(new SubtractByKeyPairFlatMapFunction(broadcast)));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.cogroup(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> broadcastCogroup(PairDataSet<K, W> rightDataSet) {
        JavaPairRDD<K,W> rightPairDataSet = ((SparkPairDataSet) rightDataSet).rdd;
        JavaSparkContext context = SpliceSpark.getContext();
        Multimap<K,W> rightBroadcast = generateMultimap(rightPairDataSet);
        final Broadcast<Multimap<K,W>> broadcast = context.broadcast(rightBroadcast);
        return new SparkPairDataSet(rdd.groupByKey().mapToPair(new CoGroupPairFunction(broadcast)));
    }

    @Override
    public PairDataSet<K, V> union(PairDataSet<K, V> dataSet) {
        return new SparkPairDataSet<>(rdd.union(((SparkPairDataSet) dataSet).rdd));
    }

    @Override
    public String toString() {
        return rdd.toString();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f) {
        return new SparkDataSet(rdd.mapPartitions(f));
    }


    @Override
    public DataSet<V> insertData(InsertTableWriterBuilder builder,OperationContext operationContext) {
        try {
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeInsertTableWriterBuilder(conf,builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            JavaSparkContext context = SpliceSpark.getContext();
            rdd.saveAsNewAPIHadoopDataset(conf);
            operationContext.getOperation().fireAfterStatementTriggers();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new SparkDataSet(context.parallelize(Collections.singletonList(new LocatedRow(valueRow))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> updateData(UpdateTableWriterBuilder builder, OperationContext operationContext) {
        try {
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeUpdateTableWriterBuilder(conf,builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            JavaSparkContext context = SpliceSpark.getContext();
            rdd.saveAsNewAPIHadoopDataset(conf);
            operationContext.getOperation().fireAfterStatementTriggers();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new SparkDataSet(context.parallelize(Collections.singletonList(new LocatedRow(valueRow))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> deleteData(DeleteTableWriterBuilder builder, OperationContext operationContext) {
        try {
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeDeleteTableWriterBuilder(conf,builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            JavaSparkContext context = SpliceSpark.getContext();
            rdd.saveAsNewAPIHadoopDataset(conf);
            operationContext.getOperation().fireAfterStatementTriggers();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new SparkDataSet(context.parallelize(Collections.singletonList(new LocatedRow(valueRow))));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
