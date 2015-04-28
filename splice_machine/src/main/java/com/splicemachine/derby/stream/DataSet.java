package com.splicemachine.derby.stream;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.function.*;

import java.util.Iterator;
import java.util.List;

/**
 *
 * Representation of a stream of data
 *
 */
public interface DataSet<V> {
    /**
     * Transform the dataset into a list of items.
     */
    List<V> collect();
    /**
     * Apply a flatmapfunction to entire partitions of data.
     */
    <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f);
    /**
     * Remove duplicates from dataset
     */
    DataSet<V> distinct();
    /**
     * Iterate over all values and produce a single value.
     */
    <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V,V,V> function2);
    /**
     * Apply function to dataset to produce an indexed dataset.  Does not require
     * uniqueness on the left values.
     */
    <Op extends SpliceOperation, K> PairDataSet<K,V> index(SplicePairFunction<Op,K,V> function);
    /**
     * Apply map function.
     */
    <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function);

    <Op extends SpliceOperation, K> PairDataSet<K,V> keyBy(SpliceFunction<Op,V,K> function);

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
    DataSet<V> union (DataSet<V> dataSet);

    /**
     * Apply a filter to the results, possible removing a row.
     *
     * @param f
     * @return
     */
    <Op extends SpliceOperation> DataSet<V> filter (SplicePredicateFunction<Op,V> f);

    DataSet<V> intersect(DataSet<V> dataSet);

    DataSet<V> subtract(DataSet<V> dataSet);

    boolean isEmpty();

   <Op extends SpliceOperation, U> DataSet<Op,U> flatMap(SpliceFlatMapFunction<Op,V, U> f);

    /**
     * Release any resources of the dataset
     *
     */
    void close();

    /**
     * Perform a fetch with offset
     *
     * @param offset
     * @param fetch
     * @return
     */
    DataSet<V> fetchWithOffset(int offset, int fetch);
 }
