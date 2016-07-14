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

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Stream of data acting on a key/values.
 */
public interface PairDataSet<K,V> {
    DataSet<V> values();
    DataSet<V> values(String name);
    DataSet<V> values(boolean isLast);
    DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);
    DataSet<K> keys();
    <Op extends SpliceOperation> PairDataSet<K,V> reduceByKey(SpliceFunction2<Op, V, V, V> function2);
    <Op extends SpliceOperation> PairDataSet<K,V> reduceByKey(SpliceFunction2<Op, V, V, V> function2,boolean isLast, boolean pushScope, String scopeDetail);
    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op, Tuple2<K, V>, U> function);
    <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> function);
    <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> function,boolean isLast);
    PairDataSet<K,V> sortByKey(Comparator<K> comparator);
    PairDataSet<K,V> sortByKey(Comparator<K> comparator,String name);
    PairDataSet<K, V> partitionBy(Partitioner<K> partitioner, Comparator<K> comparator);
    PairDataSet<K, Iterable<V>> groupByKey();
    PairDataSet<K, Iterable<V>> groupByKey(String name);
    <W> PairDataSet<K,Tuple2<V,Optional<W>>> hashLeftOuterJoin(PairDataSet<K, W> rightDataSet);
    <W> PairDataSet<K,Tuple2<Optional<V>,W>> hashRightOuterJoin(PairDataSet<K, W> rightDataSet);
    <W> PairDataSet<K,Tuple2<V,W>> hashJoin(PairDataSet<K, W> rightDataSet);
    <W> PairDataSet<K,Tuple2<V,W>> hashJoin(PairDataSet<K, W> rightDataSet,String name);
    <W> PairDataSet<K,V> subtractByKey(PairDataSet<K, W> rightDataSet);
    <W> PairDataSet<K,V> subtractByKey(PairDataSet<K, W> rightDataSet,String name);
    <W> PairDataSet<K,Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet);
    <W> PairDataSet<K,Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet,String name);
    PairDataSet<K,V> union(PairDataSet<K, V> dataSet);
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f);
    DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException;
    InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException;
    UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException;
    DataSetWriterBuilder directWriteData() throws StandardException;
    String toString();
}