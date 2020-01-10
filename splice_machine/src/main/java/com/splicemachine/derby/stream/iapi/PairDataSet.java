/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Stream of data acting on a key/values.
 */
public interface PairDataSet<K,V> {
    /**
     *
     * Return the values as a Dataset.
     *
     * @return
     */
    DataSet<V> values(OperationContext context);

    /**
     *
     * Returns the values of a dataset and overrides any
     * default name if applicable.
     *
     * @param name
     * @return
     */
    DataSet<V> values(String name, OperationContext context);

    /**
     *
     * Returns the values of a dataset and provides scope for Spark
     * UI customization.
     *
     * @param name
     * @param isLast
     * @param context
     * @param pushScope
     * @param scopeDetail
     * @return
     */
    DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    /**
     *
     * Return the keys of the dataset.
     *
     * @return
     */
    DataSet<K> keys();

    /**
     *
     * Performs a reduceByKey...
     *
     * @param function2
     * @param <Op>
     * @return
     */
    <Op extends SpliceOperation> PairDataSet<K,V> reduceByKey(SpliceFunction2<Op, V, V, V> function2);
    /**
     *
     * Performs a reduceByKey while allowing for overrides if using spark.
     *
     * @param function2
     * @param <Op>
     * @return
     */
    <Op extends SpliceOperation> PairDataSet<K,V> reduceByKey(SpliceFunction2<Op, V, V, V> function2,boolean isLast, boolean pushScope, String scopeDetail);

    /**
     *
     * Apply a map to the current Pair Data Set.
     *
     * @param function
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op, Tuple2<K, V>, U> function);
    /**
     *
     * Apply a flatmap to the current Pair Data Set.
     *
     * @param function
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> function);
    /**
     *
     * Apply a flatmap to the current Pair Data Set allowing for Spark Overrides.
     *
     * @param function
     * @param <Op>
     * @param <U>
     * @return
     */
    <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> function,boolean isLast);

    /**
     *
     * Sort utilizing the comparator provided.
     *
     * @see Comparator
     *
     * @param comparator
     * @param operationContext
     * @return
     */
    PairDataSet<K,V> sortByKey(Comparator<K> comparator, OperationContext operationContext);
    /**
     *
     * Sort by key utilizing the comparator provided.  The name can
     * be an override for Spark Implementations.
     *
     * @see Comparator
     *
     * @param comparator
     * @param operationContext
     * @return
     */
    PairDataSet<K,V> sortByKey(Comparator<K> comparator, String name, OperationContext operationContext);
    /**
     *
     * Partition the pair DataSet via a custom partitioner and comparator.
     *
     * @see org.apache.spark.Partitioner
     * @see Comparator
     *
     * @param partitioner
     * @param comparator
     * @param operationContext
     * @return
     */
    PairDataSet<K, V> partitionBy(Partitioner<K> partitioner, Comparator<K> comparator, OperationContext<JoinOperation> operationContext);
    PairDataSet<K, Iterable<V>> groupByKey(OperationContext context);
    PairDataSet<K, Iterable<V>> groupByKey(String name, OperationContext context);
    <W> PairDataSet<K,Tuple2<V,W>> hashJoin(PairDataSet<K, W> rightDataSet, OperationContext operationContext);
    <W> PairDataSet<K,Tuple2<V,W>> hashJoin(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext);
    <W> PairDataSet<K,V> subtractByKey(PairDataSet<K, W> rightDataSet, OperationContext operationContext);
    <W> PairDataSet<K,V> subtractByKey(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext);
    <W> PairDataSet<K,Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, OperationContext operationContext);
    <W> PairDataSet<K,Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, String name, OperationContext operationContext);
    PairDataSet<K,V> union(PairDataSet<K, V> dataSet);
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f);
    String toString();
    DataSetWriterBuilder directWriteData() throws StandardException;

}
