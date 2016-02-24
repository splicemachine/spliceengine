package com.splicemachine.derby.stream.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Stream of data acting on a single type of values.
 */
public interface DataSet<V> extends Iterable<V>, Serializable {

    List<V> collect();

    Future<List<V>> collectAsync(boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f);

    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast);
    
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail);
    
    DataSet<V> distinct();

    DataSet<V> distinct(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    DataSet<V> coalesce(int numPartitions, boolean shuffle);

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

    /**
     * Performs a fetch with offset
     */
    <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction);

    <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction, boolean isLast);

    <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction);

    ExportDataSetWriterBuilder writeToDisk();

    ExportDataSetWriterBuilder<String> saveAsTextFile(OperationContext operationContext);

    void persist();

    void setAttribute(String name, String value);

    String getAttribute(String name);

    void saveAsTextFile(String path);
}
