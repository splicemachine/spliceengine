package com.splicemachine.derby.stream.spark;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
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

    @SuppressWarnings("rawtypes")
    private String planIfLast(AbstractSpliceFunction f,boolean isLast){
        if(!isLast) return f.getSparkName();
        String plan=f.getOperation().getPrettyExplainPlan();
        return (plan!=null && !plan.isEmpty()?plan:f.getSparkName());
    }

    @Override
    public DataSet<V> values(){
        return values(SparkConstants.RDD_NAME_GET_VALUES);
    }

    @Override
    public DataSet<V> values(String name){
        return new SparkDataSet<>(rdd.values(),name);
    }

    @Override
    public DataSet<V> values(boolean isLast){
        return new SparkDataSet<>(rdd.values());
    }

    @Override
    public DataSet<K> keys(){
        return new SparkDataSet<>(rdd.keys());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(SpliceFunction2<Op, V, V, V> function2){
        return new SparkPairDataSet<>(rdd.reduceByKey(new SparkSpliceFunctionWrapper2<>(function2)),function2.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(SpliceFunction2<Op, V, V, V> function2,boolean isLast){
        return new SparkPairDataSet<>(rdd.reduceByKey(new SparkSpliceFunctionWrapper2<>(function2)),planIfLast(function2,isLast));
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> map(SpliceFunction<Op, Tuple2<K, V>, U> function){
        return new SparkDataSet<>(rdd.map(new SparkSpliceFunctionWrapper<>(function)),function.getSparkName());
    }

    @Override
    public PairDataSet<K, V> sortByKey(Comparator<K> comparator){
        return new SparkPairDataSet<>(rdd.sortByKey(comparator));
    }

    public PairDataSet<K, V> sortByKey(Comparator<K> comparator,String name){
        return new SparkPairDataSet<>(rdd.sortByKey(comparator),name);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(){
        return groupByKey("Group By Key");
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name){
        JavaPairRDD<K, Iterable<V>> rdd1=rdd.groupByKey();
        rdd1.setName(name);
        RDDUtils.setAncestorRDDNames(rdd1,1,new String[]{"Shuffle Data"});
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
        RDDUtils.setAncestorRDDNames(rdd1,2,new String[]{"Map Left to Right","Coalesce"});
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
        return subtractByKey(rightDataSet,SparkConstants.RDD_NAME_SUBTRACTBYKEY);
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
        return new SparkDeleteTableWriterBuilder<>(rdd);
    }

    @Override
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new SparkInsertTableWriterBuilder<>(rdd);
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new SparkUpdateTableWriterBuilder<>(rdd);
    }

    @Override
    public DataSetWriterBuilder directWriteData() throws StandardException{
        return new SparkDirectWriterBuilder<>(rdd);
    }
}
