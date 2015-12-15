package com.splicemachine.derby.stream.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Stream of data acting on a single type of values.
 *
 */
public interface DataSet<V> extends Iterable<V>, Serializable {
    /**
     * Transforms the dataset into a list of items.
     */
    List<V> collect();

    /**
     * Applies a flatmapfunction to entire partitions of data.
     */
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f);

    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast);
    
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail);
    
    /**
     * Removes duplicates from dataset
     */
    DataSet<V> distinct();

    DataSet<V> distinct(String name);

    /**
     * Decreases the number of partitions
     */
    DataSet<V> coalesce(int numPartitions, boolean shuffle);

    DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail);

    /**
     * Iterate over all values and produce a single value.
     */
    <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V,V,V> function2);
    
    <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V,V,V> function2, boolean isLast);
    
    /**
     * Applies function to dataset to produce an indexed dataset.  Does not require
     * uniqueness on the left values.
     */
    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function);
    
    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast);

    /**
     * Applies map function.
     */
    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name);

    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function, String name);

    /**
     * Returns a localiterator for computation.
     */
    Iterator<V> toLocalIterator();

    /**
     * Returns an exact count of the dataset
     *
     * @return
     */
    long count();

    /**
     * Union of two datasets
     *
     * @param dataSet
     * @return
     */
    DataSet<V> union(DataSet<V> dataSet);

    DataSet<V> union(DataSet<V> dataSet, String name);

    /**
     * Applies a filter to the results, possible removing a row.
     *
     * @param f
     * @return
     */
    <Op extends SpliceOperation> DataSet<V> filter (SplicePredicateFunction<Op,V> f);

    /**
     *
     *
     * @param dataSet
     * @return
     */
    DataSet<V> intersect(DataSet<V> dataSet);

    /**
     *
     * @param dataSet
     * @return
     */
    DataSet<V> subtract(DataSet<V> dataSet);

    /**
     *
     * @return
     */
    boolean isEmpty();

    /**
     * Apply FlatMap (1 element -> 0..n elements
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
     *
     */
    void close();

    /**
     * Performs a fetch with offset
     *
     * @return
     */
    <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction);

    <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction, boolean isLast);

    <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction);

    ExportDataSetWriterBuilder writeToDisk();

    ExportDataSetWriterBuilder<String> saveAsTextFile();


    void persist();

    void setAttribute(String name, String value);

    String getAttribute(String name);

}
