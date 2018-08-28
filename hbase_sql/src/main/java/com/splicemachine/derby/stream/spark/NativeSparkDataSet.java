/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportExecRowWriter;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import com.splicemachine.derby.stream.function.ExportFunction;
import com.splicemachine.derby.stream.function.LocatedRowToRowFunction;
import com.splicemachine.derby.stream.function.RowToLocatedRowFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.function.TakeFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.output.BulkDeleteDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.BulkInsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.BulkLoadIndexDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.pipeline.Exceptions;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.zip.GZIPOutputStream;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

/**
 *
 * DataSet Implementation for Spark.
 *
 * @see DataSet
 * @see java.io.Serializable
 *
 */
public class NativeSparkDataSet<V> implements DataSet<V> {

    private static String SPARK_COMPRESSION_OPTION = "compression";

    public Dataset<Row> dataset;
    private Map<String,String> attributes;
    private ExecRow execRow;
    public NativeSparkDataSet(Dataset<Row> dataset) {
        this.dataset = dataset;
    }


    public NativeSparkDataSet(Dataset<Row> dataset, ExecRow execRow) {
        this.dataset = dataset;
        this.execRow = execRow;
    }

    public NativeSparkDataSet(Dataset<Row> dataset, String rddname) {
        this.dataset = dataset;
//        if (dataset != null && rddname != null) this.dataset.(rddname);
    }

    public NativeSparkDataSet(JavaRDD<V> rdd, String ignored, OperationContext context) {
        try {
            this.dataset = NativeSparkDataSet.<V>toSparkRow(rdd, context);
            this.execRow = context.getOperation().getExecRowDefinition();
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
//        if (dataset != null && rddname != null) this.dataset.(rddname);
    }

    @Override
    public int partitions() {
        return dataset.rdd().getNumPartitions();
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
//   TODO     return dataset.collect();
        return (List<V>)Arrays.asList(dataset.collect());
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
//        TODO if (pushScope) SpliceSpark.pushScope(scopeDetail);
//        try {
//            return dataset.collectAsync();
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }
        return null;
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
        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<U>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).mapPartitions(new SparkFlatMapFunction<>(f)), context),f.getExecRow());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
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
//        return new SparkDataSet<>(dataset.mapPartitions(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
        return mapPartitions(f);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
//        pushScopeIfNeeded(f, pushScope, scopeDetail);
//        try {
//            return new SparkDataSet<U>(dataset.mapPartitions(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }
        return mapPartitions(f);
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
            Dataset<Row> result = dataset.distinct();

            return new NativeSparkDataSet<>(result);
        } catch (Exception se){
                throw new RuntimeException(se);
        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) {
//       return new SparkPairDataSet<>(dataset.mapToPair(new SparkSplittingFunction<>(function)), function.getSparkName());
        return null;
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, OperationContext context) {
        try {
            return new SparkPairDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).mapToPair(new SparkSplittingFunction<>(function)), function.getSparkName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast) {
//        return new SparkPairDataSet<>(dataset.mapToPair(new SparkSplittingFunction<>(function)), planIfLast(function, isLast));
        return null;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail) {
//        pushScopeIfNeeded(function, pushScope, scopeDetail);
//        try {
//            return new SparkPairDataSet(dataset.mapToPair(new SparkSplittingFunction<>(function)), planIfLast(function, isLast));
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }
        return null;
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
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> f, String name, boolean isLast, boolean pushScope, String scopeDetail) {
//        pushScopeIfNeeded(function, pushScope, scopeDetail);
//        try {
//            return new SparkDataSet<>(dataset.map(new SparkSpliceFunctionWrapper<>(function)), (name != null ? name : planIfLast(function, isLast)));
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }
        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<U>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, execRow, context).map(new SparkSpliceFunctionWrapper<>(f)), context),f.getExecRow());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    @Override
    public Iterator<V> toLocalIterator() {
//        return dataset.toLocalIterator();
        return null;
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f) {
//        return new SparkPairDataSet<>(dataset.keyBy(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
        return null;
    }

    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, String name) {
//        return new SparkPairDataSet<>(dataset.keyBy(new SparkSpliceFunctionWrapper<>(f)), name);
        return null;
    }


    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, OperationContext context) {
//        return new SparkPairDataSet<>(dataset.keyBy(new SparkSpliceFunctionWrapper<>(f)), name);

        try {
            return new SparkPairDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).keyBy(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(
            SpliceFunction<Op, V, K> f, String name, boolean pushScope, String scopeDetail) {
//
//        pushScopeIfNeeded(f, pushScope, scopeDetail);
//        try {
//            return new SparkPairDataSet(dataset.keyBy(new SparkSpliceFunctionWrapper<>(f)), name != null ? name : f.getSparkName());
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }
        return null;
    }

    @Override
    public long count() {
        return dataset.count();
    }

    @Override
    public DataSet<V> union(DataSet<V> dataSet, OperationContext operationContext) {
        return union(dataSet, operationContext, RDDName.UNION.displayName(), false, null);
    }


    @Override
    public DataSet<V> parallelProbe(List<ScanSetBuilder<ExecRow>> scanSetBuilders, OperationContext<MultiProbeTableScanOperation> operationContext) throws StandardException {
        DataSet<V> toReturn = null;
        DataSet<V> branch = null;
        int i = 0;
        MultiProbeTableScanOperation operation = operationContext.getOperation();
        for (ScanSetBuilder<ExecRow> builder: scanSetBuilders) {
            DataSet<V> dataSet = (DataSet<V>) builder.buildDataSet(operation);
            if (i % 100 == 0) {
                if (branch != null) {
                    if (toReturn == null)
                        toReturn = branch;
                    else
                        toReturn = toReturn.union(branch, operationContext);
                }
                branch = dataSet;
            }
            else
                branch = branch.union(dataSet, operationContext);
            i++;
        }
        if (branch != null) {
            if (toReturn == null)
                toReturn = branch;
            else
                toReturn = toReturn.union(branch, operationContext);
        }
        return toReturn;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet< V> union(DataSet<V> dataSet, OperationContext operationContext, String name, boolean pushScope, String scopeDetail) {
        pushScopeIfNeeded((SpliceFunction)null, pushScope, scopeDetail);
        try {
            Dataset rdd1 = dataset.union(((NativeSparkDataSet) dataSet).dataset);
//            rdd1.setName(name != null ? name : RDDName.UNION.displayName());
            return new NativeSparkDataSet<>(rdd1);
        } finally {
            if (pushScope) SpliceSpark.popScope();
        }
    }

    @Override
    public DataSet<V> orderBy(OperationContext operationContext, int[] keyColumns, boolean[] descColumns, boolean[] nullsOrderedLow) {
        try {
            Column[] orderedCols = new Column[keyColumns.length];
            for (int i = 0; i < keyColumns.length; i++) {
                Column orderCol = col(ValueRow.getNamedColumn(keyColumns[i]));
                if (descColumns[i]) {
                    if (nullsOrderedLow[i])
                        orderedCols[i] = orderCol.desc_nulls_last();
                    else
                        orderedCols[i] = orderCol.desc_nulls_first();
                }
                else {
                    if (nullsOrderedLow[i])
                        orderedCols[i] = orderCol.asc_nulls_first();
                    else
                        orderedCols[i] = orderCol.asc_nulls_last();
                }
            }
            return new NativeSparkDataSet<>(dataset.orderBy(orderedCols));
        }
        catch (Exception se){
            throw new RuntimeException(se);
        }
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
//        return new SparkDataSet<>(dataset.filter(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
        return filter(f,false, false, "");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {
//        pushScopeIfNeeded(f, pushScope, scopeDetail);
//        try {
//            return new SparkDataSet(dataset.filter(new SparkSpliceFunctionWrapper<>(f)), planIfLast(f, isLast));
//        } finally {
//            if (pushScope) SpliceSpark.popScope();
//        }

        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<V>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).filter(new SparkSpliceFunctionWrapper<>(f)), context),execRow);
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
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
            Dataset<Row> dataset = this.dataset;

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
           return new NativeSparkDataSet<>(dataset);

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
            //Convert this dataset backed iterator to a Spark untyped dataset
            Dataset<Row> left = dataset;

            //Convert the left operand to a untyped dataset
            Dataset<Row> right = ((NativeSparkDataSet)dataSet).dataset;

            //Do the intesect
            Dataset<Row> result = left.intersect(right);

            //Convert back to RDD<ExecRow>
            return new NativeSparkDataSet<V>(result);

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
            //Convert this dataset backed iterator to a Spark untyped dataset
            Dataset<Row> left = dataset;

            //Convert the right operand to a untyped dataset
            Dataset<Row> right = ((NativeSparkDataSet)dataSet).dataset;

            //Do the subtract
            Dataset<Row> result = left.except(right);

            //Convert back to RDD<ExecRow>
            return new NativeSparkDataSet<>(result);
        }
        catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }
    }



    @Override
    public boolean isEmpty() {
//        return dataset.isEmpty();
        return false;
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
//        return new SparkDataSet<>(dataset.flatMap(new SparkFlatMapFunction<>(f)), f.getSparkName());
        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<U>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).flatMap(new SparkFlatMapFunction<>(f)), context),f.getSparkName());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
//        return new SparkDataSet<>(dataset.flatMap(new SparkFlatMapFunction<>(f)), name);
        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<U>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).flatMap(new SparkFlatMapFunction<>(f)), context),f.getSparkName());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
//        return new SparkDataSet<>(dataset.flatMap(new SparkFlatMapFunction<>(f)), planIfLast(f, isLast));
        try {
            OperationContext<Op> context = f.operationContext;
            return new NativeSparkDataSet<U>(toSparkRow(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, context).flatMap(new SparkFlatMapFunction<>(f)), context),f.getSparkName());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }
    
    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction) {
//        JavaRDD<V> rdd1 = dataset.mapPartitions(new SparkFlatMapFunction<>(takeFunction));
//        rdd1.setName(takeFunction.getSparkName());
//
//        return new SparkDataSet<>(rdd1);
        return null;
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk(){
//        return new SparkExportDataSetWriter.Builder(dataset);
        return null;
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
//        dataset.saveAsTextFile(path); TODO

    }

    @Override
    public PairDataSet<V, Long> zipWithIndex() {
//        return new SparkPairDataSet<>(dataset.zipWithIndex());
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExportDataSetWriterBuilder<String> saveAsTextFile(OperationContext operationContext){
        try {
            return new SparkExportDataSetWriter.Builder<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, operationContext));
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
//        JavaRDD rdd1 = dataset.coalesce(numPartitions, shuffle);
//        rdd1.setName(String.format("Coalesce %d partitions", numPartitions));
//        SparkUtils.setAncestorRDDNames(rdd1, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"}, null);
//        return new SparkDataSet<>(rdd1);
        if (shuffle) {
            return new NativeSparkDataSet<>(dataset.repartition(numPartitions));
        } else {
            return new NativeSparkDataSet<>(dataset.coalesce(numPartitions));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
//        pushScopeIfNeeded(context, pushScope, scopeDetail);
//        try {
//            JavaRDD rdd1 = dataset.coalesce(numPartitions, shuffle);
//            rdd1.setName(String.format("Coalesce %d partitions", numPartitions));
//            SparkUtils.setAncestorRDDNames(rdd1, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"}, null);
//            return new SparkDataSet<V>(rdd1);
//        } finally {
//            if (pushScope) context.popScope();
//        }
        return coalesce(numPartitions, shuffle);
    }

    @Override
    public void persist() {
        dataset.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
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
            Dataset<Row> leftDF = dataset;
            Dataset<Row> rightDF = ((NativeSparkDataSet)rightDataSet).dataset;
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
                joinedSet =  new NativeSparkDataSet(rightDF.join(leftDF,expr,joinType.RIGHTOUTER.strategy()));
            else
                joinedSet =  new NativeSparkDataSet(leftDF.join(rightDF,expr,joinType.strategy()));
            return joinedSet;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Take a Splice DataSet and  convert to a Spark Dataset
     * doing a map
     * @param context
     * @return
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Dataset<Row> toSparkRow(JavaRDD<V> rdd, OperationContext context) throws Exception{
        return SpliceSpark.getSession()
                .createDataFrame(
                        rdd.map(new LocatedRowToRowFunction()),
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
    public static <V> JavaRDD<V> toSpliceLocatedRow(Dataset<Row> dataSet, OperationContext context) throws StandardException {
        return (JavaRDD<V>) dataSet.javaRDD().map(new RowToLocatedRowFunction(context));
    }

    public static <V> JavaRDD<V> toSpliceLocatedRow(Dataset<Row> dataSet, ExecRow execRow, OperationContext context) throws StandardException {
        if (execRow != null) {
            return (JavaRDD<V>) dataSet.javaRDD().map(new RowToLocatedRowFunction(context, execRow));
        }
        return (JavaRDD<V>) dataSet.javaRDD().map(new RowToLocatedRowFunction(context));
    }


    public static DataSet toSpliceLocatedRow(JavaRDD<Row> rdd, OperationContext context) throws StandardException {
//        return new SparkDataSet(dataset.map(new RowToLocatedRowFunction(context))); TODO
        return null;
    }

    @Override
    public DataSet<ExecRow> writeParquetFile(DataSetProcessor dsp,
                                             int[] partitionBy,
                                             String location,
                                             String compression,
                                             OperationContext context) {
        try{
            StructType dataSchema = null;
            StructType tableSchema = context.getOperation().getExecRowDefinition().schema();
            dataSchema = ExternalTableUtils.getDataSchema(dsp, tableSchema, partitionBy, location, "p");

            if (dataSchema == null)
                dataSchema = tableSchema;
            // construct a DF using schema of data
            Dataset<Row> insertDF = dataset;

            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(dataSchema.fields()[partitionBy[i]].name());
            }
            if (partitionBy.length > 0) {
                List<Column> repartitionCols = new ArrayList();
                for (int i = 0; i < partitionBy.length; i++) {
                    repartitionCols.add(new Column(dataSchema.fields()[partitionBy[i]].name()));
                }
                insertDF = insertDF.repartition(scala.collection.JavaConversions.asScalaBuffer(repartitionCols).toList());
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression)
                    .partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                    .mode(SaveMode.Append).parquet(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new NativeSparkDataSet<>(SpliceSpark.getSession().createDataFrame(Collections.singletonList(valueRow), valueRow.schema()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeAvroFile(DataSetProcessor dsp,
                                          int[] partitionBy,
                                          String location,
                                          String compression,
                                          OperationContext context) {
        try {

            StructType dataSchema = null;
            StructType tableSchema = context.getOperation().getExecRowDefinition().schema();
            dataSchema = ExternalTableUtils.getDataSchema(dsp, tableSchema, partitionBy, location, "a");

            if (dataSchema == null)
                dataSchema = tableSchema;

            Dataset<Row> insertDF = dataset;

            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(dataSchema.fields()[partitionBy[i]].name());
            }
            if (partitionBy.length > 0) {
                List<Column> repartitionCols = new ArrayList();
                for (int i = 0; i < partitionBy.length; i++) {
                    repartitionCols.add(new Column(dataSchema.fields()[partitionBy[i]].name()));
                }
                insertDF = insertDF.repartition(scala.collection.JavaConversions.asScalaBuffer(repartitionCols).toList());
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                    .mode(SaveMode.Append).format("com.databricks.spark.avro").save(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new NativeSparkDataSet<>(SpliceSpark.getSession().createDataFrame(Collections.singletonList(valueRow), valueRow.schema()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location,  String compression,
                                                    OperationContext context) {
        try {
            Dataset<Row> insertDF = dataset;

            String[] partitionByCols = new String[partitionBy.length];
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols[i] = ValueRow.getNamedColumn(partitionBy[i]);
            }
            if (partitionBy.length > 0) {
                List<Column> repartitionCols = new ArrayList();
                for (int i = 0; i < partitionBy.length; i++) {
                    repartitionCols.add(new Column(ValueRow.getNamedColumn(partitionBy[i])));
                }
                insertDF = insertDF.repartition(scala.collection.JavaConversions.asScalaBuffer(repartitionCols).toList());
            }
            insertDF.write().option(SPARK_COMPRESSION_OPTION,compression)
                    .partitionBy(partitionByCols)
                    .mode(SaveMode.Append).orc(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new NativeSparkDataSet<>(SpliceSpark.getSession().createDataFrame(Collections.singletonList(valueRow), valueRow.schema()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter,
                                                int[] baseColumnMap,
                                                OperationContext context) {

        try {
            Dataset<Row> insertDF = dataset;
            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
            insertDF.write().option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
                    .mode(SaveMode.Append).csv(location);
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
            return new NativeSparkDataSet<>(SpliceSpark.getSession().createDataFrame(Collections.singletonList(valueRow), valueRow.schema()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override @SuppressWarnings({ "unchecked", "rawtypes" })
    public void pin(ExecRow template, long conglomId) throws StandardException {
//        Dataset<Row> pinDF = SpliceSpark.getSession().createDataFrame(     TODO
//                dataset.map(new LocatedRowToRowFunction()),
//                template.schema());
//        pinDF.createOrReplaceTempView("SPLICE_"+conglomId);
//        SpliceSpark.getSession().catalog().cacheTable("SPLICE_"+conglomId);
    }

    @Override
    public DataSet<V> sampleWithoutReplacement(final double fraction) {
        return new NativeSparkDataSet<>(dataset.sample(false,fraction));
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
        return new SparkDeleteTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>(), operationContext))
                .wrapExceptions());
    }

    @Override
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new SparkInsertTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>(), operationContext))
                .wrapExceptions());
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new SparkUpdateTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>()))
                .wrapExceptions());
    }

}