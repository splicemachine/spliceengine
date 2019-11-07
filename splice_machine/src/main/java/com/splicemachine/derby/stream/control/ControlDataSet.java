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
 */

package com.splicemachine.derby.stream.control;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.ControlExecutionLimiter;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.control.output.ControlExportDataSetWriter;
import com.splicemachine.derby.stream.control.output.ParquetWriterService;
import com.splicemachine.derby.stream.function.CloneFunction;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.stream.function.MergeWindowFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.function.TakeFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.output.*;
import com.splicemachine.derby.stream.output.delete.DeletePipelineWriter;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdatePipelineWriter;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.Pair;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.Sets;
import org.spark_project.guava.io.Closeables;
import org.spark_project.guava.util.concurrent.Futures;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.splicemachine.derby.stream.control.ControlUtils.checkCancellation;

/**
 *
 * Dataset for Client Side Control Processing
 *
 * @see com.splicemachine.derby.stream.iapi.DataSet
 *
 */
public class ControlDataSet<V> implements DataSet<V> {
    protected Iterator<V> iterator;
    protected Map<String,String> attributes;
    public ControlDataSet(Iterator<V> iterator) {
        this.iterator = iterator;
    }
    
    @Override
    public int partitions() {
        return 1;
    }

    @Override
    public Pair<DataSet, Integer> materialize() {
        List<ExecRow> rows = ((ControlDataSet<ExecRow>)this).map(new CloneFunction<>(null)).collect();
        return Pair.newPair(new MaterializedControlDataSet(rows), rows.size());
    }

    @Override
    public Pair<DataSet, Integer> persistIt() {
        return materialize();
    }

    @Override
    public void unpersistIt() {
        return;
    }

    @Override
    public DataSet getClone() {
        throw new UnsupportedOperationException("Not Implemented for ControlDataSet");
    }

    @Override
    public List<V> collect() {
        return IteratorUtils.<V>toList(iterator);
    }

    @Override
    public Future<List<V>> collectAsync(boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        return Futures.immediateFuture(IteratorUtils.<V>toList(iterator));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        try {
            return new ControlDataSet<>(f.call(checkCancellation(iterator, f)));
        } catch (Exception e) {
            throw Exceptions.getRuntimeException(e);
        }
    }

    @Override
    public DataSet<V> shufflePartitions() {
        return this; //no-op
    }

    public DataSet<V> orderBy(OperationContext operationContext, int[] keyColumns, boolean[] descColumns, boolean[] nullsOrderedLow) {
        KeyerFunction f=new KeyerFunction(operationContext,keyColumns);
        PairDataSet pair=map(new CloneFunction<>(operationContext)).keyBy(f);

        PairDataSet sortedByKey=pair.sortByKey(new RowComparator(descColumns,nullsOrderedLow),
                OperationContext.Scope.SORT.displayName(), operationContext);

        return sortedByKey.values(OperationContext.Scope.READ_SORTED.displayName(), operationContext);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast) {
        return mapPartitions(f);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return mapPartitions(f);
    }

    public static <E> HashSet<E> newHashSet(Iterator<? extends E> elements, OperationContext context) {
        ControlExecutionLimiter limiter;
        if (context == null) {
            limiter = ControlExecutionLimiter.NO_OP;
        } else {
            limiter = context.getActivation().getLanguageConnectionContext().getControlExecutionLimiter();
        }
        HashSet set = Sets.newHashSet();

        while(elements.hasNext()) {
            if (set.add(elements.next()))
                limiter.addAccumulatedRows(1);
        }

        return set;
    }

    @Override
    public DataSet<V> distinct(OperationContext context) {
        return new ControlDataSet<>(newHashSet(ControlUtils.checkCancellation(iterator, context), context).iterator());
    }

    @Override
    public DataSet<V> distinct(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        return distinct(context);
    }

    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function) {
        return new ControlPairDataSet<>(Iterators.transform(checkCancellation(iterator, function),new Function<V, Tuple2<K, U>>() {
            @Nullable
            @Override
            public Tuple2<K, U> apply(@Nullable V v) {
                try {
                    return function.call(v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }));
    }


    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, OperationContext context) {
        return index(function);
    }
    
    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        return index(function);
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail) {
        return index(function);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new ControlDataSet<U>(Iterators.transform(checkCancellation(iterator, function), function));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) {
        return map(function);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name, boolean isLast, boolean pushScope, String scopeDetail) {
        return map(function);
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return iterator;
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function) {
            return new ControlPairDataSet<>(Iterators.<V,Tuple2<K, V>>transform(checkCancellation(iterator, function), new Function<V, Tuple2<K, V>>() {
                @Nullable
                @Override
                public Tuple2<K, V> apply(@Nullable V v) {
                    return Tuple2.apply(function.apply(v),v);
                }
            }));
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function, String name) {
        return keyBy(function);
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function, OperationContext context) {
        return keyBy(function);
    }


    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(
        final SpliceFunction<Op, V, K> function, String name, boolean pushScope, String scopeDetail) {
        return keyBy(function);
    }

    @Override
    public String toString() {
        return "ControlDataSet {}";
    }

    @Override
    public long count() {
        return Iterators.size(iterator);
    }

    @Override
    public DataSet<V> union(DataSet<V> dataSet, OperationContext operationContext) {
        try {
            ExecutorService es = SIDriver.driver().getExecutorService();
            ExecutorCompletionService<Iterator<V>> completionService = new ExecutorCompletionService<>(es);
            NonOrderPreservingFutureIterator<V> futureIterator = new NonOrderPreservingFutureIterator<>(completionService, 2);
            completionService.submit(new NonLazy(iterator));
            completionService.submit(new NonLazy(((ControlDataSet<V>) dataSet).iterator));
            return new ControlDataSet<>(futureIterator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> union(List<DataSet<V>> dataSetList, OperationContext operationContext) {
        try {
            ExecutorService es = SIDriver.driver().getExecutorService();
            ExecutorCompletionService<Iterator<V>> completionService = new ExecutorCompletionService<>(es);
            NonOrderPreservingFutureIterator<V> futureIterator = new NonOrderPreservingFutureIterator<>(completionService, dataSetList.size());
            for (DataSet<V> aSet: dataSetList) {
                completionService.submit(new NonLazy(((ControlDataSet<V>)aSet).iterator));
            }
            return new ControlDataSet<>(futureIterator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataSet<V> parallelProbe(List<ScanSetBuilder<ExecRow>> scanSetBuilders, OperationContext<MultiProbeTableScanOperation> operationContext) {
        ExecutorService es = SIDriver.driver().getExecutorService();
        FutureIterator<V> futureIterator = new FutureIterator<>(scanSetBuilders.size());
        for (ScanSetBuilder<ExecRow> scanSetBuilder: scanSetBuilders) {
            futureIterator.appendFutureIterator(es.submit(new NonLazy(scanSetBuilder, operationContext.getOperation())));
        }
        return new ControlDataSet<>(futureIterator);
    }

    @Override
    public DataSet< V> union(DataSet<V> dataSet, OperationContext operationContext, String name, boolean pushScope, String scopeDetail) {
        return union(dataSet, operationContext);
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new ControlDataSet<>(Iterators.filter(checkCancellation(iterator, f),f));
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return filter(f);
    }

    @Override
    public DataSet<V> intersect(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail){
        return intersect(dataSet, context);
    }

    @Override
    public DataSet< V> intersect(DataSet<V> dataSet, OperationContext context) {
        Set<V> left=newHashSet(ControlUtils.checkCancellation(iterator, context), context);
        Set<V> right=newHashSet(ControlUtils.checkCancellation(((ControlDataSet<V>)dataSet).iterator, context), context);
        Sets.SetView<V> intersection=Sets.intersection(left,right);
        return new ControlDataSet<>(intersection.iterator());
    }


    @Override
    public DataSet<V> subtract(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail){
        return subtract(dataSet, context);
    }


    @Override
    public DataSet< V> subtract(DataSet<V> dataSet, OperationContext context) {
        Set<V> left=newHashSet(ControlUtils.checkCancellation(iterator, context), context);
        Set<V> right=newHashSet(ControlUtils.checkCancellation(((ControlDataSet<V>)dataSet).iterator, context), context);
        return new ControlDataSet<>(Sets.difference(left,right).iterator());
    }

    @Override
    public boolean isEmpty() {
        return iterator.hasNext();
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new ControlDataSet(Iterators.concat(Iterators.transform(checkCancellation(iterator, f),f)));
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
        return flatMap(f);
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
        return flatMap(f);
    }

    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> f) {
        try {
            return new ControlDataSet<>(f.call(checkCancellation(iterator, f)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk(){
        return new ControlExportDataSetWriter.Builder<>(this);
    }

    @Override
    public void saveAsTextFile(String path) {
        OutputStream fileOut = null;
        try {
            DistributedFileSystem dfs = SIDriver.driver().getFileSystem(path);
            fileOut = dfs.newOutputStream(path, StandardOpenOption.CREATE);
            while (iterator.hasNext()) {
                fileOut.write(Bytes.toBytes(iterator.next().toString()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileOut !=null) {
                try {
                    Closeables.close(fileOut, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    @Override
    public PairDataSet<V, Long> zipWithIndex(OperationContext operationContext) {
        return new ControlPairDataSet<V, Long>(Iterators.<V,Tuple2<V, Long>>transform(iterator, new Function<V, Tuple2<V, Long>>() {
            long counter = 0;
            @Nullable
            @Override
            public Tuple2<V, Long> apply(@Nullable V v) {
                return new Tuple2<>(v, counter++);
            }
        }));
    }

    @Override
    @SuppressWarnings("unchecked")
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization does" +
            "not happen with control-side execution")
    public ExportDataSetWriterBuilder saveAsTextFile(OperationContext operationContext) {
        return writeToDisk()
                .exportFunction(new SpliceFunction2<SpliceOperation,OutputStream,Iterator<String>,Integer>(operationContext){
                    @Override public void writeExternal(ObjectOutput out) throws IOException{ super.writeExternal(out); }
                    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{ super.readExternal(in); }
                    @Override public SpliceOperation getOperation(){ return super.getOperation(); }
                    @Override public Activation getActivation(){ return super.getActivation(); }
                    @Override public void prepare(){ super.prepare(); }
                    @Override public void reset(){ super.reset(); }
                    @Override public String getPrettyFunctionName(){ return super.getPrettyFunctionName(); }
                    @Override public String getSparkName(){ return super.getSparkName(); }


                    @Override
                    public Integer call(OutputStream os,Iterator<String> iterator) throws Exception{
                        int size = 0;
                        while(iterator.hasNext()){
                           os.write(Bytes.toBytes(iterator.next()));
                            size++;
                        }
                        return size;
                    }
                });
    }

    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
        return this;
    }

    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        return this;
    }

    @Override
    public void persist() {
        // no op
    }

    @Override
    public void setAttribute(String name, String value) {
        if (attributes==null)
            attributes = new HashMap<>();
        attributes.put(name,value);
    }

    @Override
    public String getAttribute(String name) {
        if (attributes ==null)
            return null;
        return attributes.get(name);
    }

    @Override
    public DataSet<V> join(OperationContext operationContext, DataSet<V> rightDataSet, JoinType joinType, boolean isBroadcast) {
        throw new UnsupportedOperationException("Not Implemented in Control Side");
    }

    @Override
    public DataSet<V> crossJoin(OperationContext operationContext, DataSet<V> rightDataSet) {
        throw new UnsupportedOperationException("Not Implemented in Control Side");
    }


    /**
     * Window Function. Take a WindowContext that define the partition, the order, and the frame boundary.
     * Currently only run on top of Spark.
     * @param windowContext
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    @Override
    public DataSet<V> windows(WindowContext windowContext, OperationContext operationContext, boolean pushScope, String scopeDetail) {

        operationContext.pushScopeForOp(OperationContext.Scope.SORT_KEYER);
        KeyerFunction f = new KeyerFunction(operationContext, windowContext.getPartitionColumns());
        PairDataSet pair = keyBy(f);
        operationContext.popScope();

        operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
        pair = pair.groupByKey("Group Values For Each Key", operationContext);
        operationContext.popScope();

        operationContext.pushScopeForOp(OperationContext.Scope.EXECUTE);
        try {
            return pair.flatmap(new MergeWindowFunction(operationContext, windowContext.getWindowFunctions()), true);
        } finally {
            operationContext.popScope();
        }
    }

    /**
     *
     * Not Supported
     *
     * @param dsp
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    @Override
    public DataSet<ExecRow> writeParquetFile(DataSetProcessor dsp, int[] partitionBy, String location, String compression, OperationContext context) {

        try {
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
            RecordWriter<Void, Object> rw = ParquetWriterService.getFactory().getParquetRecordWriter(location, compression, tableSchema);

            try {
                ExpressionEncoder<Row> encoder = RowEncoder.apply(tableSchema);
                while (iterator.hasNext()) {
                    ValueRow vr = (ValueRow) iterator.next();
                    context.recordWrite();

                    rw.write(null, encoder.toRow(vr));
                }
            } finally {
                rw.close(null);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(context.getRecordsWritten()));
        return new ControlDataSet(Collections.singletonList(valueRow).iterator());
    }

    /**
     *
     * Not Supported
     *
     * @param dsp
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    @Override
    public DataSet<ExecRow> writeAvroFile(DataSetProcessor dsp, int[] partitionBy, String location, String compression, OperationContext context) {
        throw new UnsupportedOperationException("Cannot write avro files");
    }

    /**
     *
     * Not Supported
     *
     * @param baseColumnMap
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    @Override
    public DataSet<ExecRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location, String compression, OperationContext context) {
        throw new UnsupportedOperationException("Cannot write orc files");
    }

    /**
     *
     * Not Supported
     *
     * @param op
     * @param location
     * @param characterDelimiter
     * @param columnDelimiter
     * @param baseColumnMap
     * @param context
     * @return
     */
    @Override
    public DataSet<ExecRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter, int[] baseColumnMap,  OperationContext context) {
        throw new UnsupportedOperationException("Cannot write text files");
    }

    /**
     *
     * Not Supported
     *
     * @param template
     * @param conglomId
     */
    @Override
    public void pin(ExecRow template, long conglomId) {
        throw new UnsupportedOperationException("Pin Not Supported in Control Mode");
    }

    @Override
    public DataSet<V> sampleWithoutReplacement(final double fraction) {

        Iterators.filter(iterator, new Predicate<V>() {
            @Override
            public boolean apply(@Nullable V v) {
                return false;
            }
        });
        //this.
        return null;
    }

    @Override
    public BulkInsertDataSetWriterBuilder bulkInsertData(OperationContext operationContext) throws StandardException {
       throw new RuntimeException("bulk load not supported");
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder bulkLoadIndex(OperationContext operationContext) throws StandardException {
        throw new RuntimeException("bulk load not supported");
    }

    @Override
    public BulkDeleteDataSetWriterBuilder bulkDeleteData(OperationContext operationContext) throws StandardException {
        throw new RuntimeException("bulk load not supported");
    }

    @Override
    public TableSamplerBuilder sample(OperationContext operationContext) throws StandardException {
        throw new RuntimeException("sampling not supported");
    }
    /**
     *
     * Non Lazy Callable
     *
     */
    private static class NonLazy<V> implements Callable<Iterator<V>>{
        private Iterator<V> lazyIterator;
        private MultiProbeTableScanOperation operation;
        private ScanSetBuilder<ExecRow> scanSetBuilder;

        public NonLazy(ScanSetBuilder<ExecRow> scanSetBuilder, MultiProbeTableScanOperation operation) {
            this.scanSetBuilder = scanSetBuilder;
            this.operation = operation;
        }

        public NonLazy(Iterator<V> iterator) {
            this.lazyIterator = iterator;
        }

        @Override
        public Iterator<V> call() throws Exception{
            if (lazyIterator == null) {
                DataSet<ExecRow> dataSet = scanSetBuilder.buildDataSet(operation);
                lazyIterator = ((ControlDataSet) dataSet).iterator;
            }
            lazyIterator.hasNext(); // Performs hbase call - make it non lazy.
            return lazyIterator;
        }
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException{
        return new DeleteTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                DeletePipelineWriter dpw = new DeletePipelineWriter(txn,token,heapConglom,operationContext);
                dpw.setRollforward(true);
                return new ControlDataSetWriter<>((ControlDataSet<ExecRow>)ControlDataSet.this,dpw,operationContext, updateCounts);
            }
        };
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new InsertTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                InsertPipelineWriter ipw = new InsertPipelineWriter(pkCols,
                        tableVersion,
                        execRowDefinition,
                        autoIncrementRowLocationArray,
                        spliceSequences,
                        heapConglom,
                        txn,
                        token, operationContext,
                        isUpsert);
                ipw.setRollforward(true);
                return new ControlDataSetWriter<>((ControlDataSet<ExecRow>)ControlDataSet.this,ipw,operationContext,updateCounts);
            }
        }.operationContext(operationContext);
    }

    @Override
    @SuppressFBWarnings(value = "SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION",justification = "Serialization" +
            "of Control-side operations does not happen and would be a mistake")
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new UpdateTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null: "Txn is null";
                UpdatePipelineWriter upw =new UpdatePipelineWriter(heapConglom,
                        formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                        txn,token,execRowDefinition,heapList,operationContext);

                upw.setRollforward(true);
                return new ControlDataSetWriter<>((ControlDataSet<ExecRow>)ControlDataSet.this,upw,operationContext, updateCounts);
            }
        };
    }

    @Override
    public DataSet upgradeToSparkNativeDataSet(OperationContext operationContext) {
         return this;
    }

    @Override
    public DataSet applyNativeSparkAggregation(int[] groupByColumns, SpliceGenericAggregator[] aggregates, boolean isRollup, OperationContext operationContext) { return null; }
}
