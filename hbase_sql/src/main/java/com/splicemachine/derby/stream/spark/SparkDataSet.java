/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportExecRowWriter;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import com.splicemachine.derby.stream.function.CountWriteFunction;
import com.splicemachine.derby.stream.function.ExportFunction;
import com.splicemachine.derby.stream.function.LocatedRowToRowFunction;
import com.splicemachine.derby.stream.function.LocatedRowToRowAvroFunction;
import com.splicemachine.derby.stream.function.RowToLocatedRowFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.function.TakeFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.BulkDeleteDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.BulkInsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.utils.AvroUtils;
import com.splicemachine.derby.stream.output.*;
import com.splicemachine.utils.ByteDataInput;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;

import static org.apache.spark.sql.functions.broadcast;

/**
 *
 * DataSet Implementation for Spark.
 *
 * @see com.splicemachine.derby.stream.iapi.DataSet
 * @see java.io.Serializable
 *
 */
public class SparkDataSet<V> implements DataSet<V> {

    private static String SPARK_COMPRESSION_OPTION = "compression";

    public JavaRDD<V> rdd;
    private Map<String,String> attributes;
    public SparkDataSet(JavaRDD<V> rdd) {
        this.rdd = rdd;
    }

    public SparkDataSet(JavaRDD<V> rdd, String rddname) {
        this.rdd = rdd;
        if (rdd != null && rddname != null) this.rdd.setName(rddname);
    }

    @Override
    public int partitions() {
        return rdd.getNumPartitions();
    }

    /**
     *
     * Execute the job and materialize the results as a List.  Be careful, all
     * data must be held in memory.
     *
     * @return
     */
    @Override
    public List<V> collect() {
        return rdd.collect();
    }

    /**
     *
     * Perform the execution asynchronously and returns a Future<List>.  Be careful, all
     * data must be held in memory.
     *
     * @param isLast
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    @Override
    public Future<List<V>> collectAsync(boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        if (pushScope) SpliceSpark.pushScope(scopeDetail);
        try {
            return rdd.collectAsync();
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    /**
     *
     * Wraps a function on an entire partition.
     *
     * @see JavaRDD#mapPartitions(FlatMapFunction)
     *
     * @param f
     * @param <Op>
     * @param <U>
     * @return
     */
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        return new SparkDataSet<>(rdd.mapPartitions(new SparkFlatMapFunction<>(f)),f.getSparkName());
    }

    /**
     *
     * Wraps a function on an entire partition.  IsLast is used for visualizing results.
     *
     * @param f
     * @param isLast
     * @param <Op>
     * @param <U>
     * @return
     */
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast) {
        return new SparkDataSet<>(rdd.mapPartitions(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(f, pushScope, scopeDetail);
        try {
            return new SparkDataSet<U>(rdd.mapPartitions(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    /**
     *
     *
     * @see JavaRDD#distinct()
     *
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> distinct(OperationContext context) {
        return distinct("Remove Duplicates", false, context, false, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> distinct(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(context, pushScope, scopeDetail);
        try {
            Dataset<Row> result = toSparkRow(this,context)
                           .distinct();

            return toSpliceLocatedRow(result,context);

        }
         catch (Exception se){
                throw new RuntimeException(se);

        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) {
       return new SparkPairDataSet<>(rdd.mapToPair(new SparkSplittingFunction<>(function)), function.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        return new SparkPairDataSet<>(rdd.mapToPair(new SparkSplittingFunction<>(function)), planIfLast(function, isLast));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(function, pushScope, scopeDetail);
        try {
            return new SparkPairDataSet(rdd.mapToPair(new SparkSplittingFunction<>(function)), planIfLast(function, isLast));
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return map(function, null, false, false, null);
    }

    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) {
        return map(function, null, isLast, false, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name, boolean isLast, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(function, pushScope, scopeDetail);
        try {
            return new SparkDataSet<>(rdd.map(new SparkSpliceFunctionWrapper<>(function)), (name != null ? name : planIfLast(function, isLast)));
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return rdd.toLocalIterator();
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f) {
        return new SparkPairDataSet<>(rdd.keyBy(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
    }

    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, String name) {
        return new SparkPairDataSet<>(rdd.keyBy(new SparkSpliceFunctionWrapper<>(f)), name);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(
            SpliceFunction<Op, V, K> f, String name, boolean pushScope, String scopeDetail) {

        pushScopeIfNeeded(f, pushScope, scopeDetail);
        try {
            return new SparkPairDataSet(rdd.keyBy(new SparkSpliceFunctionWrapper<>(f)), name != null ? name : f.getSparkName());
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @Override
    public long count() {
        return rdd.count();
    }

    @Override
    public DataSet<V> union(DataSet< V> dataSet) {
        return union(dataSet, RDDName.UNION.displayName(), false, null);
    }


    @Override
    public DataSet<V> parallelProbe(List<DataSet<V>> dataSets) {
        DataSet<V> toReturn = null;
        DataSet<V> branch = null;
        int i = 0;
        for (DataSet<V> dataSet: dataSets) {
            if (i % 100 == 0) {
                if (branch != null) {
                    if (toReturn == null)
                        toReturn = branch;
                    else
                        toReturn = toReturn.union(branch);
                }
                branch = dataSet;
            }
            else
                branch = branch.union(dataSet);
            i++;
        }
        if (branch != null) {
            if (toReturn == null)
                toReturn = branch;
            else
                toReturn = toReturn.union(branch);
        }
        return toReturn;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet< V> union(DataSet< V> dataSet, String name, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded((SpliceFunction)null, pushScope, scopeDetail);
        try {
            JavaRDD rdd1 = rdd.union(((SparkDataSet) dataSet).rdd);
            rdd1.setName(name != null ? name : RDDName.UNION.displayName());
            return new SparkDataSet<>(rdd1);
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new SparkDataSet<>(rdd.filter(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(f, pushScope, scopeDetail);
        try {
            return new SparkDataSet(rdd.filter(new SparkSpliceFunctionWrapper<>(f)), planIfLast(f, isLast));
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet< V> intersect(DataSet<V> dataSet, OperationContext context) {
        return intersect(dataSet,"Intersect Operator",context,false,null);
    }

    /**
     * Spark implementation of Window function,
     * We convert the derby specification using SparkWindow helper
     * Most of the specifications is identical to Spark except the one position index
     * and some specific functions. Look at SparkWindow for more
     * @param windowContext
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    public DataSet<V> windows(WindowContext windowContext, OperationContext context,  boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(context, pushScope, scopeDetail);
        try {
            Dataset<Row> dataset = toSparkRow(this,context);

            for(WindowAggregator aggregator : windowContext.getWindowFunctions()) {
                // we need to remove to convert resultColumnId from a 1 position index to a 0position index
                DataType resultDataType = dataset.schema().fields()[aggregator.getResultColumnId()-1].dataType();
                // We define the window specification and we get a back a spark.
                // Simply provide all the information and spark window will build it for you
                Column col = SparkWindow.partitionBy(aggregator.getPartitions())
                        .function(aggregator.getType())
                        .inputs(aggregator.getInputColumnIds())
                        .orderBy(aggregator.getOrderings())
                        .frameBoundary(aggregator.getFrameDefinition())
                        .specificArgs(aggregator.getFunctionSpecificArgs())
                        .resultColumn(aggregator.getResultColumnId())
                        .resultDataType(resultDataType)
                        .toColumn();

                // Now we replace the result column by the spark specification.
                // the result column is already define by derby. We need to replace it
                dataset = dataset.withColumn(ValueRow.getNamedColumn(aggregator.getResultColumnId()-1),col);
            }
            //Convert back to Splice Row
           return  toSpliceLocatedRow(dataset, context);

        } catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet< V> intersect(DataSet< V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(context, pushScope, scopeDetail);
        try {
            //Convert this rdd backed iterator to a Spark untyped dataset
            Dataset<Row> left = SpliceSpark.getSession()
                    .createDataFrame(
                        rdd.map(
                            new LocatedRowToRowFunction()),
                        context.getOperation()
                               .getExecRowDefinition()
                               .schema());

            //Convert the left operand to a untyped dataset
            Dataset<Row> right = SpliceSpark.getSession()
                    .createDataFrame(
                            ((SparkDataSet)dataSet).rdd
                                   .map(new LocatedRowToRowFunction()),
                            context.getOperation()
                                   .getExecRowDefinition()
                                   .schema());

            //Do the intesect
            Dataset<Row> result = left.intersect(right);

            //Convert back to RDD<ExecRow>
            return toSpliceLocatedRow(result,context);

        }
        catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> subtract(DataSet<V> dataSet, OperationContext context){
        return subtract(dataSet,"Substract/Except Operator",context,false,null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet< V> subtract(DataSet< V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(context, pushScope, scopeDetail);
        try {
            //Convert this rdd backed iterator to a Spark untyped dataset
            Dataset<Row> left = toSparkRow(this,context);

            //Convert the right operand to a untyped dataset
            Dataset<Row> right = toSparkRow(dataSet,context);

            //Do the subtract
            Dataset<Row> result = left.except(right);

            //Convert back to RDD<ExecRow>
            return toSpliceLocatedRow(result, context);
        }
        catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }
    }



    @Override
    public boolean isEmpty() {
        return rdd.take(1).isEmpty();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new SparkDataSet<>(rdd.flatMap(new SparkFlatMapFunction<>(f)), f.getSparkName());
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
        return new SparkDataSet<>(rdd.flatMap(new SparkFlatMapFunction<>(f)), name);
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
        return new SparkDataSet<>(rdd.flatMap(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
    }
    
    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction) {
        JavaRDD<V> rdd1 = rdd.mapPartitions(new SparkFlatMapFunction<>(takeFunction));
        rdd1.setName(takeFunction.getSparkName());

        return new SparkDataSet<>(rdd1);
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk(){
        return new SparkExportDataSetWriter.Builder(rdd);
    }

    public static class EOutputFormat extends FileOutputFormat<Void, ExecRow> {

        /**
         * Overridden to avoid throwing an exception if the specified directory
         * for export already exists.
         */
        @Override
        public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
            Path outDir = getOutputPath(job);
            if(outDir == null) {
                throw new InvalidJobConfException("Output directory not set.");
            } else {
                TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outDir}, job.getConfiguration());
                /*
                if(outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
                    System.out.println("Output dir already exists, no problem");
                    throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
                }
                */
            }
        }

        @Override
        public RecordWriter<Void, ExecRow> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration conf = taskAttemptContext.getConfiguration();
            String encoded = conf.get("exportFunction");
            ByteDataInput bdi = new ByteDataInput(
            Base64.decodeBase64(encoded));
            SpliceFunction2<ExportOperation, OutputStream, Iterator<ExecRow>, Void> exportFunction;
            try {
                exportFunction = (SpliceFunction2<ExportOperation, OutputStream, Iterator<ExecRow>, Void>) bdi.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }

            final ExportOperation op = exportFunction.getOperation();
            CompressionCodec codec = null;
            String extension = ".csv";
            boolean isCompressed = op.getExportParams().isCompression();
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        getOutputCompressorClass(taskAttemptContext, GzipCodec.class);
                codec =ReflectionUtils.newInstance(codecClass, conf);
                extension += ".gz";
            }

            Path file = getDefaultWorkFile(taskAttemptContext, extension);
            FileSystem fs = file.getFileSystem(conf);
            OutputStream fileOut = fs.create(file, false);
            if (isCompressed) {
                fileOut = new GZIPOutputStream(fileOut);
            }
            final ExportExecRowWriter rowWriter = ExportFunction.initializeRowWriter(fileOut, op.getExportParams());
            return new RecordWriter<Void, ExecRow>() {
                @Override
                public void write(Void _, ExecRow locatedRow) throws IOException, InterruptedException {
                    try {
                        rowWriter.writeRow(locatedRow, op.getSourceResultColumnDescriptors());
                    } catch (StandardException e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                    rowWriter.close();
                }
            };
        }
    }

    @Override
    public void saveAsTextFile(String path) {
        rdd.saveAsTextFile(path);
    }

    @Override
    public PairDataSet<V, Long> zipWithIndex() {
        return new SparkPairDataSet<>(rdd.zipWithIndex());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExportDataSetWriterBuilder<String> saveAsTextFile(OperationContext operationContext){
        return new SparkExportDataSetWriter.Builder<>(rdd);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
        JavaRDD rdd1 = rdd.coalesce(numPartitions, shuffle);
        rdd1.setName(String.format("Coalesce %d partitions", numPartitions));
        SparkUtils.setAncestorRDDNames(rdd1, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"}, null);
        return new SparkDataSet<>(rdd1);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded(context, pushScope, scopeDetail);
        try {
            JavaRDD rdd1 = rdd.coalesce(numPartitions, shuffle);
            rdd1.setName(String.format("Coalesce %d partitions", numPartitions));
            SparkUtils.setAncestorRDDNames(rdd1, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"}, null);
            return new SparkDataSet<V>(rdd1);
        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public void persist() {
        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
    }

    @Override
    public void setAttribute(String name, String value) {
        if (attributes == null)
            attributes = new HashMap<>();
        attributes.put(name,value);
    }

    @Override
    public String getAttribute(String name) {
        if (attributes == null)
            return null;
        return attributes.get(name);
    }

    private void pushScopeIfNeeded(AbstractSpliceFunction function, boolean pushScope, String scopeDetail) {
        if (pushScope) {
            if (function != null && function.operationContext != null)
                function.operationContext.pushScopeForOp(scopeDetail);
            else
                SpliceSpark.pushScope(scopeDetail);
        }
    }

    private void pushScopeIfNeeded(OperationContext operationContext, boolean pushScope, String scopeDetail) {
        if (pushScope) {
            if (operationContext != null)
                operationContext.pushScopeForOp(scopeDetail);
            else
                SpliceSpark.pushScope(scopeDetail);
        }
    }

    @SuppressWarnings("rawtypes")
    private String planIfLast(AbstractSpliceFunction f, boolean isLast) {
        if (!isLast) return f.getSparkName();
        String plan = f.getOperation()==null?null:f.getOperation().getPrettyExplainPlan();
        return (plan != null && !plan.isEmpty() ? plan : f.getSparkName());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> join(OperationContext context, DataSet<V> rightDataSet, JoinType joinType, boolean isBroadcast) {
        try {
            JoinOperation op = (JoinOperation) context.getOperation();
            Dataset<Row> leftDF = SpliceSpark.getSession().createDataFrame(
                    rdd.map(new LocatedRowToRowFunction()),
                            context.getOperation().getLeftOperation().getExecRowDefinition().schema());
            Dataset<Row> rightDF = SpliceSpark.getSession().createDataFrame(
                    ((SparkDataSet)rightDataSet).rdd.map(new LocatedRowToRowFunction()),
                    context.getOperation().getRightOperation().getExecRowDefinition().schema());
                if (isBroadcast) {
                    rightDF = broadcast(rightDF);
                }
            Column expr = null;
            int[] rightJoinKeys = ((JoinOperation)context.getOperation()).getRightHashKeys();
            int[] leftJoinKeys = ((JoinOperation)context.getOperation()).getLeftHashKeys();
            assert rightJoinKeys!=null && leftJoinKeys!=null && rightJoinKeys.length == leftJoinKeys.length:"Join Keys Have Issues";
            for (int i = 0; i< rightJoinKeys.length;i++) {
                Column joinEquality = (leftDF.col(ValueRow.getNamedColumn(leftJoinKeys[i]))
                        .equalTo(rightDF.col(ValueRow.getNamedColumn(rightJoinKeys[i]))));
                expr = i!=0?expr.and(joinEquality):joinEquality;
            }
            DataSet joinedSet;

            if (op.wasRightOuterJoin)
                joinedSet =  new SparkDataSet(rightDF.join(leftDF,expr,joinType.RIGHTOUTER.strategy()).rdd().toJavaRDD().map(
                        new RowToLocatedRowFunction(context)));
            else
                joinedSet =  new SparkDataSet(leftDF.join(rightDF,expr,joinType.strategy()).rdd().toJavaRDD().map(
                    new RowToLocatedRowFunction(context)));
            return joinedSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Take a Splice DataSet and  convert to a Spark Dataset
     * doing a map
     * @param dataSet
     * @param context
     * @return
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    Dataset<Row> toSparkRow(DataSet< V> dataSet, OperationContext context) throws Exception{
        return SpliceSpark.getSession()
                .createDataFrame(
                        ((SparkDataSet)dataSet).rdd
                                .map(new LocatedRowToRowFunction()),
                        context.getOperation()
                                .getExecRowDefinition()
                                .schema());
    }

    /**
     * Take a spark dataset and translate that to Splice format
     * @param dataSet
     * @param context
     * @return
     */

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static DataSet toSpliceLocatedRow(Dataset<Row> dataSet, OperationContext context) throws StandardException {
        return new SparkDataSet(dataSet.javaRDD()
                .map(new RowToLocatedRowFunction(context)));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeParquetFile(int[] baseColumnMap, int[] partitionBy, String location,  String compression,
                                          OperationContext context) {
        try {
            Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(
                    rdd.map(new SparkSpliceFunctionWrapper<>(new CountWriteFunction(context))).map(new LocatedRowToRowFunction()),
                    context.getOperation().getExecRowDefinition().schema());

            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                    cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(ValueRow.getNamedColumn(partitionBy[i]));
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                    .mode(SaveMode.Append).parquet(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeAvroFile(int[] baseColumnMap, int[] partitionBy, String location,  String compression,
                                                OperationContext context) {
        try {

            StructType schema = AvroUtils.supportAvroDateType(context.getOperation().getExecRowDefinition().schema(),"a");

            Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(
                    rdd.map(new SparkSpliceFunctionWrapper<>(new CountWriteFunction(context))).map(new LocatedRowToRowAvroFunction()),
                    schema);

            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(ValueRow.getNamedColumn(partitionBy[i]));
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                    .mode(SaveMode.Append).format("com.databricks.spark.avro").save(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location,  String compression,
                                                    OperationContext context) {
        try {
            Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(
                    rdd.map(new SparkSpliceFunctionWrapper<>(new CountWriteFunction(context))).map(new LocatedRowToRowFunction()),
                    context.getOperation().getExecRowDefinition().schema());
            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            String[] partitionByCols = new String[partitionBy.length];
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols[i] = ValueRow.getNamedColumn(partitionBy[i]);
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression)
                    .partitionBy(partitionByCols)
                    .mode(SaveMode.Append).orc(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter,
                                                int[] baseColumnMap,
                                                OperationContext context) {

        try {
            Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(
                    rdd.map(new SparkSpliceFunctionWrapper<>(new CountWriteFunction(context))).map(new LocatedRowToRowFunction()),
                    context.getOperation().getExecRowDefinition().schema());
            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
            insertDF.write().option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
                    .mode(SaveMode.Append).csv(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override @SuppressWarnings({ "unchecked", "rawtypes" })
    public void pin(ExecRow template, long conglomId) throws StandardException {
        Dataset<Row> pinDF = SpliceSpark.getSession().createDataFrame(
                rdd.map(new LocatedRowToRowFunction()),
                template.schema());
        pinDF.createOrReplaceTempView("SPLICE_"+conglomId);
        SpliceSpark.getSession().catalog().cacheTable("SPLICE_"+conglomId);
    }

    @Override
    public DataSet<V> sampleWithoutReplacement(final double fraction) {
        return new SparkDataSet<>(rdd.sample(false,fraction));
    }

    @Override
    public BulkInsertDataSetWriterBuilder bulkInsertData(OperationContext operationContext) throws StandardException {
        return new SparkBulkInsertTableWriterBuilder(this);
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder bulkLoadIndex(OperationContext operationContext) throws StandardException {
        return new SparkBulkLoadIndexDataSetWriterBuilder(this);
    }

    @Override
    public BulkDeleteDataSetWriterBuilder bulkDeleteData(OperationContext operationContext) throws StandardException {
        return new SparkBulkDeleteTableWriterBuilder(this);
    }

    @Override
    public DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException{
        return new SparkDeleteTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>()))
                .wrapExceptions());
    }

    @Override
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new SparkInsertTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>()))
                .wrapExceptions());
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new SparkUpdateTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>()))
                .wrapExceptions());
    }

}
