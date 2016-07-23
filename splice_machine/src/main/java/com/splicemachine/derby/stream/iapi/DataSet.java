/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Stream of data acting on an iterable set of values.
 */
public interface DataSet<V> extends Iterable<V>, Serializable {
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
    DataSet<V> distinct();

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

    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function);
    
    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast);

    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name, boolean isLast, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, String name);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, String name, boolean pushScope, String scopeDetail);

    /**
     * Returns a local iterator for computation.
     */
    Iterator<V> toLocalIterator();

    /**
     * Returns an exact count of the dataset
     */
    long count();

    DataSet<V> union(DataSet<V> dataSet);

    DataSet<V> union(DataSet<V> dataSet, String name, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation> DataSet<V> filter(SplicePredicateFunction<Op,V> f);

    <Op extends SpliceOperation> DataSet<V> filter(SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail);

    DataSet<V> intersect(DataSet<V> dataSet);

    DataSet<V> subtract(DataSet<V> dataSet);

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

    PairDataSet<V, Long> zipWithIndex();
}
