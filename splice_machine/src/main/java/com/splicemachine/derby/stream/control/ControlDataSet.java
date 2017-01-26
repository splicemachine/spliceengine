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

package com.splicemachine.derby.stream.control;

import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.commons.collections.IteratorUtils;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.*;
import org.spark_project.guava.util.concurrent.Futures;
import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.output.ControlExportDataSetWriter;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import org.spark_project.guava.io.Closeables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.spark_project.guava.util.concurrent.ThreadFactoryBuilder;
import scala.Tuple2;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

import static com.splicemachine.derby.stream.control.ControlUtils.entryToTuple;

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
            return new ControlDataSet<>(f.call(iterator));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast) {
        return mapPartitions(f);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return mapPartitions(f);
    }

    @Override
    public DataSet<V> distinct() {
        return new ControlDataSet<>(Sets.<V>newHashSet(iterator).iterator());
    }

    @Override
    public DataSet<V> distinct(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        return distinct();
    }

    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function) {
        return new ControlPairDataSet<>(Iterators.transform(iterator,new Function<V, Tuple2<K, U>>() {
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
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        return index(function);
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail) {
        return index(function);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new ControlDataSet<U>(Iterators.transform(iterator, function));
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
        return new ControlPairDataSet<>(entryToTuple(Multimaps.index(iterator,function).entries()));
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function, String name) {
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
    public DataSet<V> union(DataSet< V> dataSet) {
        ThreadPoolExecutor tpe = null;
        try {

        ThreadFactory factory=new ThreadFactoryBuilder()
                .setNameFormat("union-begin-query-%d")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler(){
                    @Override
                    public void uncaughtException(Thread t,Throwable e){
                        e.printStackTrace();
                    }
                })
                .build();
        tpe=new ThreadPoolExecutor(2,2,
                60, TimeUnit.SECONDS,new SynchronousQueue<Runnable>(),factory,
                new ThreadPoolExecutor.CallerRunsPolicy());
        tpe.allowCoreThreadTimeOut(false);
        tpe.prestartAllCoreThreads();
        Future<Iterator<V>> leftSideFuture = tpe.submit(new NonLazy(iterator));
        Future<Iterator<V>> rightSideFuture = tpe.submit(new NonLazy(((ControlDataSet<V>) dataSet).iterator));

        return new ControlDataSet<>(Iterators.concat(leftSideFuture.get(), rightSideFuture.get()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
        if (tpe!=null)
            tpe.shutdown();
        }
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet, String name, boolean pushScope, String scopeDetail) {
        return union(dataSet);
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new ControlDataSet<>(Iterators.filter(iterator,f));
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return filter(f);
    }

    @Override
    public DataSet<V> intersect(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail){
        return intersect(dataSet);
    }

    @Override
    public DataSet< V> intersect(DataSet< V> dataSet) {
        Set<V> left=Sets.newHashSet(iterator);
        Set<V> right=Sets.newHashSet(((ControlDataSet<V>)dataSet).iterator);
        Sets.SetView<V> intersection=Sets.intersection(left,right);
        return new ControlDataSet<>(intersection.iterator());
    }


    @Override
    public DataSet<V> subtract(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail){
        return subtract(dataSet);
    }


    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        Set<V> left=Sets.newHashSet(iterator);
        Set<V> right=Sets.newHashSet(((ControlDataSet<V>)dataSet).iterator);
        return new ControlDataSet<>(Sets.difference(left,right).iterator());
    }

    @Override
    public boolean isEmpty() {
        return iterator.hasNext();
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new ControlDataSet(Iterators.concat(Iterators.transform(iterator,f)));
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
            return new ControlDataSet<>(f.call(iterator));
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
            DistributedFileSystem dfs = SIDriver.driver().fileSystem();
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
    public PairDataSet<V, Long> zipWithIndex() {
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
    public Iterator<V> iterator() {
        return this.toLocalIterator();
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
        pair = pair.groupByKey("Group Values For Each Key");
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
     * @param baseColumnMap
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    @Override
    public DataSet<LocatedRow> writeParquetFile(int[] baseColumnMap, int[] partitionBy, String location, String compression, OperationContext context) {
        throw new UnsupportedOperationException("Cannot write parquet files");
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
    public DataSet<LocatedRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location, String compression, OperationContext context) {
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
    public DataSet<LocatedRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter, int[] baseColumnMap,  OperationContext context) {
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

    /**
     * Not Supported
     * @param conglomId
     */
    @Override
    public void dropPin(long conglomId) {
        throw new UnsupportedOperationException("Un Pin Not Supported in Control Mode");
    }

    /**
     *
     * Non Lazy Callable
     *
     */
    private class NonLazy implements Callable<Iterator<V>>{
        private final Iterator<V> lazyIterator;

        public NonLazy(Iterator<V> lazyIterator){
            this.lazyIterator=lazyIterator;
        }

        @Override
        public Iterator<V> call() throws Exception{
            lazyIterator.hasNext(); // Performs hbase call - make it non lazy.
            return lazyIterator;
        }
    }


}
