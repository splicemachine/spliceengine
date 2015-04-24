package com.splicemachine.derby.stream;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.*;

import java.util.Iterator;
import java.util.List;

/**
 *
 * Representation of a stream of data
 *
 */
public interface DataSet<Op extends SpliceOperation,V> {
    /**
     * Transform the dataset into a list of items.
     */
    List<V> collect();
    /**
     * Apply a flatmapfunction to entire partitions of data.
     */
    <U> DataSet<Op,U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f);
    /**
     * Remove duplicates from dataset
     */
    DataSet<Op,V> distinct();
    /**
     * Iterate over all values and produce a single value.
     */
    V fold(V zeroValue, SpliceFunction2<Op,V,V,V> function2);
    /**
     * Apply function to dataset to produce an indexed dataset.  Does not require
     * uniqueness on the left values.
     */
    <K> PairDataSet<Op,K,V> index(SplicePairFunction<Op,K,V> function);
    /**
     * Apply map function.
     */
    <U> DataSet<Op,U> map(SpliceFunction<Op,V,U> function);

    <K> PairDataSet<Op,K,V> keyBy(SpliceFunction<Op,V,K> function);

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
    DataSet<Op,V> union (DataSet<Op,V> dataSet);

    /**
     * Apply a filter to the results, possible removing a row.
     *
     * @param f
     * @return
     */
    DataSet<Op,V> filter (SplicePredicateFunction<Op,V> f);

    DataSet<Op,V> intersect(DataSet<Op,V> dataSet);

    DataSet<Op,V> subtract(DataSet<Op,V> dataSet);

    boolean isEmpty();

   <U> DataSet<Op,U> flatMap(SpliceFlatMapFunction<Op,V, U> f);
 }