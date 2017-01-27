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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;
import scala.util.Either;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * The Key Value Dataset operations for Spark.  These mimic the
 * existing Spark Key Value Operations in JavaPairRDD.
 *
 * @see org.apache.spark.api.java.JavaPairRDD
 */
public class SparkPairDataSet<K,V> implements PairDataSet<K, V>{

    public JavaPairRDD<K, V> rdd;

    public SparkPairDataSet(JavaPairRDD<K, V> rdd){
        this.rdd=rdd;
    }

    public SparkPairDataSet(JavaPairRDD<K, V> rdd,String rddName){
        this.rdd=rdd;
        if(rdd!=null && rddName!=null) this.rdd.setName(rddName);
    }

    @Override
    public DataSet<V> values(){
        return values(RDDName.GET_VALUES.displayName());
    }

    @Override
    public DataSet<V> values(String name){
        return new SparkDataSet<>(rdd.values(),name);
    }

    @Override
    public DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        if (pushScope) context.pushScopeForOp(scopeDetail);
        try {
            return new SparkDataSet<V>(rdd.values(), name != null ? name : RDDName.GET_VALUES.displayName());
        } finally {
            if (pushScope) context.popScope();
        }
    }

    @Override
    public DataSet<K> keys(){
        return new SparkDataSet<>(rdd.keys());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(SpliceFunction2<Op, V, V, V> function2){
        return reduceByKey(function2, false, false, null);    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(
        SpliceFunction2<Op,V, V, V> function2, boolean isLast, boolean pushScope, String scopeDetail) {

        pushScopeIfNeeded(function2, pushScope, scopeDetail);
        try {
            return new SparkPairDataSet<>(rdd.reduceByKey(new SparkSpliceFunctionWrapper2<>(function2)), planIfLast(function2, isLast));
        } finally {
            if (pushScope) function2.operationContext.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> map(SpliceFunction<Op, Tuple2<K, V>, U> function){
        return new SparkDataSet<>(rdd.map(new SparkSpliceFunctionWrapper<>(function)),function.getSparkName());
    }

    @Override
    public PairDataSet<K, V> sortByKey(Comparator<K> comparator){
        return sortByKey(comparator, "Sort By Key");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public PairDataSet< K, V> sortByKey(Comparator<K> comparator, String name) {
        JavaPairRDD rdd2 = rdd.sortByKey(comparator);
        rdd2.setName(name);
     return new SparkPairDataSet<>(rdd2);
    }

    @Override
    public PairDataSet<K, V> partitionBy(Partitioner<K> partitioner, Comparator<K> comparator) {
        partitioner.initialize();
        JavaPairRDD rdd2 = rdd.repartitionAndSortWithinPartitions((org.apache.spark.Partitioner) partitioner, comparator);
        return new SparkPairDataSet<>(rdd2);
    }


    @Override
    public PairDataSet<K, Iterable<V>> groupByKey() {
        return groupByKey("Group By Key");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name) {
        JavaPairRDD rdd1 = rdd.groupByKey();
        rdd1.setName(name);
        SparkUtils.setAncestorRDDNames(rdd1, 1, new String[]{"Shuffle Data"}, null);
        return new SparkPairDataSet<>(rdd1);
    }

    @Override
    public <W> PairDataSet<K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(PairDataSet<K, W> rightDataSet){
        return new SparkPairDataSet<>(rdd.leftOuterJoin(((SparkPairDataSet<K,W>)rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet<K, W> rightDataSet){
        return new SparkPairDataSet<>(rdd.rightOuterJoin(((SparkPairDataSet<K,W>)rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<V, W>> hashJoin(PairDataSet<K, W> rightDataSet){
        return hashJoin(rightDataSet,"Hash Join");
    }

    @Override
    public <W> PairDataSet<K, Tuple2<V, W>> hashJoin(PairDataSet<K, W> rightDataSet,String name){
        JavaPairRDD<K, Tuple2<V, W>> rdd1=rdd.join(((SparkPairDataSet<K, W>)rightDataSet).rdd);
        rdd1.setName(name);
        SparkUtils.setAncestorRDDNames(rdd1,2,new String[]{"Map Left to Right","Coalesce"}, null);
        return new SparkPairDataSet<>(rdd1);
    }

    private <W> Multimap<K, W> generateMultimap(JavaPairRDD<K, W> rightPairDataSet){
        Multimap<K, W> returnValue=ArrayListMultimap.create();
        List<Tuple2<K, W>> value=rightPairDataSet.collect();
        for(Tuple2<K, W> tuple : value){
            returnValue.put(tuple._1,tuple._2);
        }
        return returnValue;
    }

    private <W> Set<K> generateKeySet(JavaPairRDD<K, W> rightPairDataSet){
        return rightPairDataSet.collectAsMap().keySet();
    }

    @Override
    public <W> PairDataSet<K, V> subtractByKey(PairDataSet<K, W> rightDataSet){
        return subtractByKey(rightDataSet,RDDName.SUBTRACT_BY_KEY.displayName());
    }

    @Override
    public <W> PairDataSet<K, V> subtractByKey(PairDataSet<K, W> rightDataSet,String name){
        return new SparkPairDataSet<>(rdd.subtractByKey(((SparkPairDataSet<K,W>)rightDataSet).rdd),name);
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> f){
        return new SparkDataSet<>(rdd.flatMap(new SparkFlatMapFunction<>(f)),f.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K, V>, U> f,boolean isLast){
        return new SparkDataSet<>(rdd.flatMap(new SparkFlatMapFunction<>(f)),planIfLast(f,isLast));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet){
        return new SparkPairDataSet<>(rdd.cogroup(((SparkPairDataSet<K,W>)rightDataSet).rdd));
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet,String name){
        JavaPairRDD<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup=rdd.cogroup(((SparkPairDataSet<K, W>)rightDataSet).rdd);
        return new SparkPairDataSet<>(cogroup,name);
    }

    @Override
    public PairDataSet<K, V> union(PairDataSet<K, V> dataSet){
        return new SparkPairDataSet<>(rdd.union(((SparkPairDataSet<K, V>)dataSet).rdd));
    }

    @Override
    public String toString(){
        return rdd.toString();
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f){
        return new SparkDataSet<>(rdd.mapPartitions(new SparkFlatMapFunction<>(f)),f.getSparkName());
    }

    @Override
    public DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException{
        return new SparkDeleteTableWriterBuilder<>(wrapExceptions());
    }

    @Override
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new SparkInsertTableWriterBuilder<>(wrapExceptions());
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new SparkUpdateTableWriterBuilder<>(wrapExceptions());
    }

    @Override
    public DataSetWriterBuilder directWriteData() throws StandardException{
        return new SparkDirectWriterBuilder<>(wrapExceptions());
    }

    private void pushScopeIfNeeded(AbstractSpliceFunction function, boolean pushScope, String scopeDetail) {
        if (pushScope) {
            if (function != null && function.operationContext != null)
                function.operationContext.pushScopeForOp(scopeDetail);
            else
                SpliceSpark.pushScope(scopeDetail);
        }
    }

    JavaPairRDD<K,Either<Exception, V>> wrapExceptions() {
        return rdd.mapPartitionsToPair(new ExceptionWrapperFunction<K, V>());
    }

    @SuppressWarnings("rawtypes")
    private String planIfLast(AbstractSpliceFunction f,boolean isLast){
        if(!isLast) return f.getSparkName();
        String plan = (f.getOperation() != null ? f.getOperation().getPrettyExplainPlan() : null);
        return (plan!=null && !plan.isEmpty()?plan:f.getSparkName());
    }
}
