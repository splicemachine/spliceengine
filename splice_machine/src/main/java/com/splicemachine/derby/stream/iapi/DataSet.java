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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.output.*;
import com.splicemachine.utils.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Stream of data acting on an iterable set of values.
 */
public interface DataSet<V> extends //Iterable<V>,
        Serializable {

    int partitions();

    Pair<DataSet, Integer> materialize();

    Pair<DataSet, Integer> persistIt();

    DataSet getClone();

    void unpersistIt();

    public enum JoinType {
        INNER("inner"),
        OUTER("outer"),
        FULL("full"),
        FULLOUTER("fullouter"),
        LEFTOUTER("leftouter"),
        RIGHTOUTER("rightouter"),
        RIGHT("right"),
        LEFTSEMI("leftsemi"),
        LEFTANTI("leftanti");

        private final String strategy;

        JoinType(String strategy) {
            this.strategy = strategy;
        }

        public String strategy() {
            return strategy;
        }
    }

    /**
     *
     * Collect the stream of data, materializes to a list (memory beware).
     *
     * @return
     */
    List<V> collect();

    /**
     *
     * Collect the stream of data to a list asynchronously (memare beware).
     *
     * @param isLast
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    Future<List<V>> collectAsync(boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    /**
     *
     * Perform a function on the entire partition and provide an iterator out of the function.
     *
     * @param f
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f);


    /**
     *
     * Shuffle partitions in no-cost operation
     * @return
     */
    DataSet<V> shufflePartitions();

    /**
     *
     * Perform a function on the entire partition and provide an iterator out of the function.
     *
     * @param f
     * @param isLast
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast);

    /**
     *
     * Perform a function on the entire partition and provide an iterator out of the function.
     *
     * @param f
     * @param isLast
     * @param pushScope
     * @param scopeDetail
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail);

    /**
     *
     * Perform a distinct on all elements of the dataset.
     *
     * @return
     */
    DataSet<V> distinct(OperationContext context);

    /**
     *
     * Perform a distinct on all elements of the dataset.  Adding information
     * here to hack the Spark UI to see custom labels.
     *
     * @param name
     * @param isLast
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    DataSet<V> distinct(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    /**
     *
     * Change the number of partitions of the data if applicable.  Be careful to
     * consider Performance (Shuffle) and memory (number of Partitions) when
     * performing this operation.
     *
     * @param numPartitions
     * @param shuffle
     * @return
     */
    DataSet<V> coalesce(int numPartitions, boolean shuffle);

    /**
     *
     * Change the number of partitions of the data if applicable.  Be careful to
     * consider Performance (Shuffle) and memory (number of Partitions) when
     * performing this operation.  Additonal methods are added here to hack
     * the Spark UI.
     *
     * @param numPartitions
     * @param shuffle
     * @param isLast
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) throws StandardException;

    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, OperationContext context) throws StandardException;
    
    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast);

    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) throws StandardException;

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) throws StandardException;

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name, boolean isLast, boolean pushScope, String scopeDetail) throws StandardException;

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, String name);
    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, OperationContext context) throws StandardException;

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, String name, boolean pushScope, String scopeDetail);

    /**
     * Returns a local iterator for computation.
     */
    Iterator<V> toLocalIterator();

    /**
     * Returns an exact count of the dataset
     */
    long count();

    DataSet<V> union(DataSet<V> dataSet, OperationContext operationContext);

    DataSet<V> union(List<DataSet<V>> dataSetList, OperationContext operationContext);

    DataSet<V> orderBy(OperationContext operationContext, int[] keyColumns, boolean[] descColumns, boolean[] nullsOrderedLow);

    DataSet<V> parallelProbe(List<ScanSetBuilder<ExecRow>> dataSets, OperationContext<MultiProbeTableScanOperation> operationContext) throws StandardException;

    DataSet<V> union(DataSet<V> dataSet, OperationContext operationContext, String name, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation> DataSet<V> filter(SplicePredicateFunction<Op,V> f);

    <Op extends SpliceOperation> DataSet<V> filter(SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail);

    DataSet<V> intersect(DataSet<V> dataSet, OperationContext context) throws StandardException;

    DataSet<V> intersect(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail) throws StandardException;

    DataSet<V> subtract(DataSet<V> dataSet, OperationContext context) throws StandardException;

    DataSet<V> subtract(DataSet<V> dataSet, String name, OperationContext context, boolean pushScope, String scopeDetail) throws StandardException;

    boolean isEmpty();

    /**
     * Applies FlatMap (1 element -> 0..n elements
     *
     * @param f
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> flatMap(SpliceFlatMapFunction<Op,V, U> f);

    <Op extends SpliceOperation, U> DataSet<U> flatMap(SpliceFlatMapFunction<Op,V, U> f, String name);

    <Op extends SpliceOperation, U> DataSet<U> flatMap(SpliceFlatMapFunction<Op,V, U> f, boolean isLast);
   
    /**
     * Releases any resources of the dataset
     */
    void close();
    
    <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op, V> takeFunction);

    ExportDataSetWriterBuilder writeToDisk();

    ExportDataSetWriterBuilder<String> saveAsTextFile(OperationContext operationContext);

    void persist();

    void setAttribute(String name, String value);

    String getAttribute(String name);

    void saveAsTextFile(String path);

    PairDataSet<V, Long> zipWithIndex(OperationContext operationContext) throws StandardException;

    DataSet<V> join(OperationContext operationContext, DataSet<V> rightDataSet,JoinType joinType, boolean isBroadcast) throws StandardException;

    DataSet<V> crossJoin(OperationContext operationContext, DataSet<V> rightDataSet) throws StandardException;

    /**
     *  Window Function abstraction. Take a window context that defines the the partition, the sorting , the frame boundary
     *  and the differents functions
     * @param windowContext
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */

    DataSet<V> windows(WindowContext windowContext, OperationContext context, boolean pushScope, String scopeDetail) throws StandardException;

    /**
     *
     * Write Parquet File to the Hadoop Filesystem compliant location.
     *
     * @param dsp
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    DataSet<ExecRow> writeParquetFile(DataSetProcessor dsp, int[] partitionBy, String location, String compression,
                                         OperationContext context) throws StandardException;

    /**
     *
     * Write Avro File to the Hadoop Filesystem compliant location.
     *
     * @param dsp
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    DataSet<ExecRow> writeAvroFile(DataSetProcessor dsp, int[] partitionBy, String location,
                                   String compression, OperationContext context) throws StandardException;


    /**
     *
     * Write ORC file to the Hadoop compliant location.
     *
     * @param baseColumnMap
     * @param partitionBy
     * @param location
     * @param context
     * @return
     */
    DataSet<ExecRow> writeORCFile(int[] baseColumnMap, int[] partitionBy, String location, String compression,
                                     OperationContext context) throws StandardException;

    /**
     *
     * Write text file to the Hadoop compliant location.
     *
     * @param op
     * @param location
     * @param characterDelimiter
     * @param columnDelimiter
     * @param baseColumnMap
     * @param context
     * @return
     */
    DataSet<ExecRow> writeTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter, int[] baseColumnMap,
                                      OperationContext context) throws StandardException;

    /**
     *
     * Pin the conglomerate with the table definition (ExecRow) into memory.
     *
     * @param template
     * @param conglomId
     */
    void pin(ExecRow template, long conglomId) throws StandardException;


    DataSet<V> sampleWithoutReplacement(final double fraction);

    BulkInsertDataSetWriterBuilder bulkInsertData(OperationContext operationContext) throws StandardException;

    BulkDeleteDataSetWriterBuilder bulkDeleteData(OperationContext operationContext) throws StandardException;

    BulkLoadIndexDataSetWriterBuilder bulkLoadIndex(OperationContext operationContext) throws StandardException;
    DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException;
    InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException;
    UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException;
    TableSamplerBuilder sample(OperationContext operationContext) throws StandardException;

    DataSet upgradeToSparkNativeDataSet(OperationContext operationContext) throws StandardException;

    DataSet applyNativeSparkAggregation(int[] groupByColumns, SpliceGenericAggregator[] aggregates, boolean isRollup, OperationContext operationContext);
}
