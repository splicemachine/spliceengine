/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportExecRowWriter;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportFile.COMPRESSION;
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
import com.splicemachine.derby.stream.iapi.TableSamplerBuilder;
import com.splicemachine.derby.stream.output.BulkDeleteDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.BulkInsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.BulkLoadIndexDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.spark.splicemachine.ShuffleUtils;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.Pair;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import javax.annotation.Nullable;
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
    private OperationContext context;

    public NativeSparkDataSet(Dataset<Row> dataset) {
        this(dataset, (OperationContext) null);
    }
    
    public NativeSparkDataSet(Dataset<Row> dataset, OperationContext context) {
        int cols = dataset.columns().length;
        String[] colNames = new String[cols];
        for (int i = 0; i<cols; i++) {
            colNames[i] = "c"+i;
        }
        this.dataset = dataset.toDF(colNames);
        this.context = context;
    }


    public NativeSparkDataSet(Dataset<Row> dataset, ExecRow execRow) {
        this(dataset);
        this.execRow = execRow;
    }

    public NativeSparkDataSet(Dataset<Row> dataset, String rddname) {
        this(dataset);
    }

    public NativeSparkDataSet(JavaRDD<V> rdd, String ignored, OperationContext context) {
        this(NativeSparkDataSet.<V>toSparkRow(rdd, context));
        try {
            this.execRow = context.getOperation().getExecRowDefinition();
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    // Build a Native Spark Dataset from the context of the consumer operation.
    // Only works for an op with a single child op... not joins.
    public NativeSparkDataSet(JavaRDD<V> rdd, OperationContext context) {
        this(NativeSparkDataSet.<V>sourceRDDToSparkRow(rdd, context), context);
        try {
            this.execRow = context.getOperation().getLeftOperation().getExecRowDefinition();
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    @Override
    public int partitions() {
        return dataset.rdd().getNumPartitions();
    }

    @Override
    public Pair<DataSet, Integer> materialize() {
        return Pair.newPair(this, (int) dataset.count());
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
        throw new UnsupportedOperationException();
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
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).mapPartitions(f);

        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    @Override
    public DataSet<V> shufflePartitions() {
        return new NativeSparkDataSet(ShuffleUtils.shuffle(dataset), this.context);
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
        return mapPartitions(f);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
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

            return new NativeSparkDataSet<>(result, context);
        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) throws StandardException {
        return new SparkDataSet(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).index(function);
    }

    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, OperationContext context) throws StandardException {
        return new SparkPairDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context).mapToPair(new SparkSplittingFunction<>(function)), function.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) throws StandardException {
        return map(function, null, false, false, null);
    }

    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) throws StandardException {
        return map(function, null, isLast, false, null);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> f, String name, boolean isLast, boolean pushScope, String scopeDetail) throws StandardException {
        OperationContext<Op> context = f.operationContext;
        if (f.hasNativeSparkImplementation()) {
            Pair<Dataset<Row>, OperationContext> pair = f.nativeTransformation(dataset, context);
            OperationContext c = pair.getSecond() == null ? this.context : pair.getSecond();
            return new NativeSparkDataSet<>(pair.getFirst(), c);
        }
        return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, f.getExecRow(), context)).map(f, name, isLast, pushScope, scopeDetail);
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return Iterators.transform(dataset.toLocalIterator(), new Function<Row, V>() {
            @Nullable
            @Override
            public V apply(@Nullable Row input) {
                ValueRow vr = new ValueRow(input.size());
                vr.fromSparkRow(input);
                return (V) vr;
            }
        });
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f) {
        try {
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).keyBy(f);
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, String name) {
        return keyBy(f);
    }


    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, OperationContext context) throws StandardException {
        return new SparkPairDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context).keyBy(new SparkSpliceFunctionWrapper<>(f)), f.getSparkName());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(
            SpliceFunction<Op, V, K> f, String name, boolean pushScope, String scopeDetail) {
        return keyBy(f);
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
            Dataset right = getDataset(dataSet);
            Dataset rdd1 = dataset.union(right);
            return new NativeSparkDataSet<>(rdd1, operationContext);
        } catch (Exception se){
            throw new RuntimeException(se);
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
            return new NativeSparkDataSet<>(dataset.orderBy(orderedCols), operationContext);
        }
        catch (Exception se){
            throw new RuntimeException(se);
        }
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return filter(f,false, false, "");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {

        OperationContext<Op> context = f.operationContext;
        if (f.hasNativeSparkImplementation()) {
            // TODO:  Enable the commented try-catch block after regression testing.
            //        This would be a safeguard against unanticipated exceptions:
            //             org.apache.spark.sql.catalyst.parser.ParseException
            //             org.apache.spark.sql.AnalysisException
            //    ... which may occur if the Splice parser fails to detect a
            //        SQL expression which SparkSQL does not support.
//          try {
                Pair<Dataset<Row>, OperationContext> pair = f.nativeTransformation(dataset, context);
                OperationContext c = pair.getSecond() == null ? this.context : pair.getSecond();
                return new NativeSparkDataSet<>(pair.getFirst(), c);
//          }
//          catch (Exception e) {
//               Switch back to non-native DataSet if the SparkSQL filter is not supported.
//          }
        }
        try {
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).filter(f, isLast, pushScope, scopeDetail);
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
           return new NativeSparkDataSet<>(dataset, context);

        } catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }

    }

    private Dataset getDataset(DataSet dataSet) throws StandardException {
        if (dataSet instanceof NativeSparkDataSet) {
            return ((NativeSparkDataSet) dataSet).dataset;
        } else {
            //Convert the right operand to a untyped dataset
            return SpliceSpark.getSession()
                    .createDataFrame(
                            ((SparkDataSet)dataSet).rdd
                                    .map(new LocatedRowToRowFunction()),
                            context.getOperation()
                                    .getExecRowDefinition()
                                    .schema());
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
            Dataset<Row> right = getDataset(dataSet);

            //Do the intesect
            Dataset<Row> result = left.intersect(right);

            return new NativeSparkDataSet<V>(result, context);

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
            Dataset<Row> right = getDataset(dataSet);

            //Do the subtract
            Dataset<Row> result = left.except(right);

            return new NativeSparkDataSet<>(result, context);
        }
        catch (Exception se){
            throw new RuntimeException(se);
        }finally {
            if (pushScope) context.popScope();
        }
    }



    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("isEmpty() not supported");
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        try {
            OperationContext<Op> context = f.operationContext;
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).flatMap(f);
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
        try {
            OperationContext<Op> context = f.operationContext;
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).flatMap(f, name);
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
        try {
            OperationContext<Op> context = f.operationContext;
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).flatMap(f, isLast);
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }
    
    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk() {
        try {
            return new SparkDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context)).writeToDisk();
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
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
            COMPRESSION compression = op.getExportParams().getCompression();
            if (compression == COMPRESSION.BZ2) {
                extension += ".bz2";
            }
            else if (compression == COMPRESSION.GZ) {
                extension += ".gz";
            }

            Path file = getDefaultWorkFile(taskAttemptContext, extension);
            FileSystem fs = file.getFileSystem(conf);
            OutputStream fileOut = fs.create(file, false);
            if (compression == COMPRESSION.BZ2) {
                CompressionCodecFactory factory = new CompressionCodecFactory(conf);
                codec = factory.getCodecByClassName("org.apache.hadoop.io.compress.BZip2Codec");
                fileOut = codec.createOutputStream(fileOut);
            }
            else if (compression == COMPRESSION.GZ) {
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
        throw new UnsupportedOperationException("saveAsTextFile() not supported");
    }

    @Override
    public PairDataSet<V, Long> zipWithIndex(OperationContext context) throws StandardException {
        return new SparkPairDataSet<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context).zipWithIndex());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ExportDataSetWriterBuilder<String> saveAsTextFile(OperationContext operationContext){
        try {
            return new SparkExportDataSetWriter.Builder<>(NativeSparkDataSet.<V>toSpliceLocatedRow(dataset, this.context));
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
        if (shuffle) {
            return new NativeSparkDataSet<>(dataset.repartition(numPartitions), this.context);
        } else {
            return new NativeSparkDataSet<>(dataset.coalesce(numPartitions), this.context);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
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
            Dataset<Row> rightDF;
            if (rightDataSet instanceof NativeSparkDataSet) {
                rightDF = ((NativeSparkDataSet)rightDataSet).dataset;
            } else {
                rightDF = SpliceSpark.getSession().createDataFrame(
                        ((SparkDataSet)rightDataSet).rdd.map(new LocatedRowToRowFunction()),
                        context.getOperation().getRightOperation().getExecRowDefinition().schema());
            }

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
                joinedSet =  new NativeSparkDataSet(rightDF.join(leftDF,expr,joinType.RIGHTOUTER.strategy()), context);
            else
                joinedSet =  new NativeSparkDataSet(leftDF.join(rightDF,expr,joinType.strategy()), context);
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
    static <V> Dataset<Row> toSparkRow(JavaRDD<V> rdd, OperationContext context) {
        try {
            return SpliceSpark.getSession()
                    .createDataFrame(
                            rdd.map(new LocatedRowToRowFunction()),
                            context.getOperation()
                                    .getExecRowDefinition()
                                    .schema());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
    }

    /**
     * Take a Splice DataSet in the consumer's context, with a single source,
     * and convert it to a Spark Dataset doing a map.
     * @param context
     * @return
     * @throws Exception
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static <V> Dataset<Row> sourceRDDToSparkRow(JavaRDD<V> rdd, OperationContext context) {
        try {
            return SpliceSpark.getSession()
                    .createDataFrame(
                            rdd.map(new LocatedRowToRowFunction()),
                            context.getOperation().getLeftOperation()
                                    .getExecRowDefinition()
                                    .schema());
        } catch (Exception e) {
            throw Exceptions.throwAsRuntime(e);
        }
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


    @Override
    public DataSet<ExecRow> writeParquetFile(DataSetProcessor dsp,
                                             int[] partitionBy,
                                             String location,
                                             String compression,
                                             OperationContext context) throws StandardException {
        //Generate Table Schema
        String[] colNames;
        DataValueDescriptor[] dvds;
        if (context.getOperation() instanceof DMLWriteOperation) {
            dvds  = context.getOperation().getExecRowDefinition().getRowArray();
            colNames = ((DMLWriteOperation) context.getOperation()).getColumnNames();
        } else if (context.getOperation() instanceof ExportOperation) {
            dvds = context.getOperation().getLeftOperation().getLeftOperation().getExecRowDefinition().getRowArray();
            ExportOperation export = (ExportOperation) context.getOperation();
            ResultColumnDescriptor[] descriptors = export.getSourceResultColumnDescriptors();
            colNames = new String[descriptors.length];
            int i = 0;
            for (ResultColumnDescriptor rcd : export.getSourceResultColumnDescriptors()) {
                colNames[i++] = rcd.getName();
            }
        } else {
            throw new IllegalArgumentException("Unsupported operation type: " + context.getOperation());
        }
        StructField[] fields = new StructField[colNames.length];
        for (int i=0 ; i<colNames.length ; i++){
            fields[i] = dvds[i].getStructField(colNames[i]);
        }
        StructType tableSchema = DataTypes.createStructType(fields);

        StructType dataSchema = ExternalTableUtils.getDataSchema(dsp, tableSchema, partitionBy, location, "p");

        if (dataSchema == null)
            dataSchema = tableSchema;

        // construct a DF using schema of data
        Dataset<Row> insertDF = SpliceSpark.getSession()
                .createDataFrame(dataset.rdd(), dataSchema);

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
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeAvroFile(DataSetProcessor dsp,
                                          int[] partitionBy,
                                          String location,
                                          String compression,
                                          OperationContext context) throws StandardException {
        StructType dataSchema = null;

        //Generate Table Schema
        String[] colNames = ((DMLWriteOperation) context.getOperation()).getColumnNames();
        DataValueDescriptor[] dvds = context.getOperation().getExecRowDefinition().getRowArray();
        StructField[] fields = new StructField[colNames.length];
        for (int i=0 ; i<colNames.length ; i++){
            fields[i] = dvds[i].getStructField(colNames[i]);
        }
        StructType tableSchema = DataTypes.createStructType(fields);
        dataSchema = ExternalTableUtils.getDataSchema(dsp, tableSchema, partitionBy, location, "a");


        if (dataSchema == null)
            dataSchema = tableSchema;

        Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(dataset.rdd(), dataSchema);

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
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }


    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location,  String compression,
                                                    OperationContext context) throws StandardException {
        //Generate Table Schema
        String[] colNames = ((DMLWriteOperation) context.getOperation()).getColumnNames();
        DataValueDescriptor[] dvds = context.getOperation().getExecRowDefinition().getRowArray();
        StructField[] fields = new StructField[colNames.length];
        for (int i=0 ; i<colNames.length ; i++){
            fields[i] = dvds[i].getStructField(colNames[i]);
        }
        StructType tableSchema = DataTypes.createStructType(fields);

        Dataset<Row> insertDF = SpliceSpark.getSession().createDataFrame(
                dataset.rdd(),
                tableSchema);

        String[] partitionByCols = new String[partitionBy.length];
        for (int i = 0; i < partitionBy.length; i++) {
            partitionByCols[i] = colNames[partitionBy[i]];
        }
        if (partitionBy.length > 0) {
            List<Column> repartitionCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                repartitionCols.add(new Column(colNames[partitionBy[i]]));
            }
            insertDF = insertDF.repartition(scala.collection.JavaConversions.asScalaBuffer(repartitionCols).toList());
        }
        insertDF.write().option(SPARK_COMPRESSION_OPTION,compression)
                .partitionBy(partitionByCols)
                .mode(SaveMode.Append).orc(location);
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<ExecRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter,
                                                int[] baseColumnMap,
                                                OperationContext context) {
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
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @Override @SuppressWarnings({ "unchecked", "rawtypes" })
    public void pin(ExecRow template, long conglomId) throws StandardException {
        dataset.createOrReplaceTempView("SPLICE_"+conglomId);
        SpliceSpark.getSession().catalog().cacheTable("SPLICE_"+conglomId);
    }

    @Override
    public DataSet<V> sampleWithoutReplacement(final double fraction) {
        return new NativeSparkDataSet<>(dataset.sample(false,fraction), this.context);
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
        return new SparkInsertTableWriterBuilder<>(this);
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new SparkUpdateTableWriterBuilder<>(((SparkPairDataSet) this.index(new EmptySparkPairDataSet<>()))
                .wrapExceptions());
    }

    @Override
    public TableSamplerBuilder sample(OperationContext operationContext) throws StandardException {
        return new SparkTableSamplerBuilder(this, this.context);
    }

    @Override
    public DataSet upgradeToSparkNativeDataSet(OperationContext operationContext) {
         return this;
    }

}