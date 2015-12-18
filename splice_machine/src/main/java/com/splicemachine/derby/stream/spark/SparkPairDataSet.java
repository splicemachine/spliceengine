package com.splicemachine.derby.stream.spark;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.index.HTableOutputFormat;
import com.splicemachine.derby.stream.index.HTableWriterBuilder;
import com.splicemachine.derby.stream.output.SMOutputFormat;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.pipeline.exception.ErrorState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

    public SparkPairDataSet(JavaPairRDD<K,V> rdd, String rddName) {
        this.rdd = rdd;
        if (rdd != null && rddName != null) this.rdd.setName(rddName);
    }

    @SuppressWarnings("rawtypes")
    private String planIfLast(AbstractSpliceFunction f, boolean isLast) {
        if (!isLast) return f.getSparkName();
        String plan = f.getOperation().getPrettyExplainPlan();
        return (plan != null && !plan.isEmpty() ? plan : f.getSparkName());
    }
    
    @Override
    public DataSet<V> values() {
        return values(SparkConstants.RDD_NAME_GET_VALUES);
    }

    @Override
    public DataSet<V> values(String name) {
        return new SparkDataSet<V>(rdd.values(), name);
    }

    @Override
    public DataSet<V> values(boolean isLast) {
        return new SparkDataSet<V>(rdd.values());
    }

    @Override
    public DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        if (pushScope) context.pushScopeForOp(scopeDetail);
        try {
            return new SparkDataSet<V>(rdd.values(), name != null ? name : SparkConstants.RDD_NAME_GET_VALUES);
        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public DataSet<K> keys() {
        return new SparkDataSet<K>(rdd.keys());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(SpliceFunction2<Op,V, V, V> function2) {
        return reduceByKey(function2, false, false, null);
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(
        SpliceFunction2<Op,V, V, V> function2, boolean isLast, boolean pushScope, String scopeDetail) {
        if (pushScope) function2.operationContext.pushScopeForOp(scopeDetail);
        try {
            return new SparkPairDataSet<>(rdd.reduceByKey(function2), planIfLast(function2, isLast));
        } finally {
            if (pushScope) function2.operationContext.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new SparkDataSet<U>(rdd.map(function), function.getSparkName());
    }

    @Override
    public PairDataSet< K, V> sortByKey(Comparator<K> comparator) {
        return new SparkPairDataSet<>(rdd.sortByKey(comparator));
    }

    public PairDataSet< K, V> sortByKey(Comparator<K> comparator, String name) {
        return new SparkPairDataSet<>(rdd.sortByKey(comparator), name);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey() {
        return groupByKey("Group By Key");
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name) {
        JavaPairRDD rdd1 = rdd.groupByKey();
        rdd1.setName(name);
        RDDUtils.setAncestorRDDNames(rdd1, 1, new String[]{"Shuffle Data"});
        return new SparkPairDataSet<>(rdd1);
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
        return hashJoin(rightDataSet, "Hash Join");
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet, String name) {
        JavaPairRDD rdd1 = rdd.join(((SparkPairDataSet) rightDataSet).rdd);
        rdd1.setName(name);
        RDDUtils.setAncestorRDDNames(rdd1, 2, new String[]{"Map Left to Right", "Coalesce"});
        return new SparkPairDataSet(rdd1);
    }

    private <W> Multimap<K,W> generateMultimap(JavaPairRDD<K,W> rightPairDataSet) {
        Multimap<K,W> returnValue = ArrayListMultimap.create();
        List<Tuple2<K,W>> value = rightPairDataSet.collect();
        for (Tuple2<K,W> tuple: value) {
            returnValue.put(tuple._1,tuple._2);
        }
        return returnValue;
    }

    private <W> Set<K> generateKeySet(JavaPairRDD<K,W> rightPairDataSet) {
        return rightPairDataSet.collectAsMap().keySet();
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet) {
        return subtractByKey(rightDataSet, SparkConstants.RDD_NAME_SUBTRACTBYKEY);
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet, String name) {
        return new SparkPairDataSet(rdd.subtractByKey(((SparkPairDataSet) rightDataSet).rdd), name);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> f) {
        return new SparkDataSet<U>(rdd.flatMap(f), f.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> f, boolean isLast) {
        return new SparkDataSet<U>(rdd.flatMap(f), planIfLast(f, isLast));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        return new SparkPairDataSet(rdd.cogroup(((SparkPairDataSet) rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, String name) {
        JavaPairRDD rdd1 = rdd.cogroup(((SparkPairDataSet) rightDataSet).rdd);
        return new SparkPairDataSet(rdd1, name);
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
        return new SparkDataSet(rdd.mapPartitions(f), f.getSparkName());
    }


    @Override
    public DataSet<V> insertData(InsertTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
        try {
            if (operationContext.getOperation() != null) {
                operationContext.getOperation().fireBeforeStatementTriggers();
            }
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeInsertTableWriterBuilder(conf,builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            rdd.saveAsNewAPIHadoopDataset(conf);
            if (operationContext.getOperation() != null) {
                operationContext.getOperation().fireAfterStatementTriggers();
            }
            ValueRow valueRow = new ValueRow(1);
            InsertOperation insertOperation = ((InsertOperation)operationContext.getOperation());
            if (insertOperation!=null && insertOperation.isImport()) {
                List<String> badRecords = operationContext.getBadRecords();
                if (badRecords.size()>0) {
                    // System.out.println("badRecords -> " + badRecords);
                    DataSet dataSet = new ControlDataSet<>(badRecords);
                    Path path = null;
                    if (insertOperation.statusDirectory != null && !insertOperation.statusDirectory.equals("NULL")) {
                        FileSystem fileSystem = FileSystem.get(SpliceConstants.config);
                        path = ImportUtils.generateFileSystemPathForWrite(insertOperation.statusDirectory, fileSystem, insertOperation);
                        dataSet.saveAsTextFile(path.toString());
                        fileSystem.close();
                    }
                    if (insertOperation.failBadRecordCount==0)
                        throw new RuntimeException(badRecords.get(0));
                    if (badRecords.size()>= insertOperation.failBadRecordCount)
                        throw new RuntimeException(ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(path==null?"--No Output File Provided--":path.toString()));
                }
            }

            valueRow.setColumn(1,new SQLInteger((int)operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> updateData(UpdateTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
        try {
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeUpdateTableWriterBuilder(conf, builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            rdd.saveAsNewAPIHadoopDataset(conf);
            operationContext.getOperation().fireAfterStatementTriggers();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> deleteData(DeleteTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
        try {
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeDeleteTableWriterBuilder(conf, builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, SMOutputFormat.class, SMOutputFormat.class);
            rdd.saveAsNewAPIHadoopDataset(conf);
            operationContext.getOperation().fireAfterStatementTriggers();
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> writeKVPair(HTableWriterBuilder builder){

        try {
            Configuration conf = new Configuration(SIConstants.config);
            TableWriterUtils.serializeHTableWriterBuilder(conf, builder);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, HTableOutputFormat.class, HTableOutputFormat.class);
            JavaSparkContext context = SpliceSpark.getContext();
            rdd.saveAsNewAPIHadoopDataset(conf);
            ValueRow valueRow = new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger(0));
            return new SparkDataSet(context.parallelize(Collections.singletonList(new LocatedRow(valueRow))));
        } catch (Exception e) {
            throw new RuntimeException(Throwables.getRootCause(e));
        }
    }
}
