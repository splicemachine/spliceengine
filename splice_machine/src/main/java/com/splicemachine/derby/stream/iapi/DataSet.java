package com.splicemachine.derby.stream.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.*;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Stream of data acting on a single type of values.
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
     * Decrease the number of partitions
     *
     */
    DataSet<V> coalesce(int numPartitions, boolean shuffle);

    /**
     * Iterate over all values and produce a single value.
     */
    <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V,V,V> function2);
    /**
     * Apply function to dataset to produce an indexed dataset.  Does not require
     * uniqueness on the left values.
     */
    <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function);
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

    /**
     * Release any resources of the dataset
     *
     */
    void close();

    /**
     * Perform a fetch with offset
     *
     * @return
     */
    <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction);

    <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction);

//    DataSet<LocatedRow> writeToDisk(ExportParams exportParams);

    <Op extends SpliceOperation> DataSet<LocatedRow> writeToDisk(String directory, SpliceFunction2<Op, OutputStream, Iterator<V>, Integer> exportFunction);

    void saveAsTextFile(String path);

    void persist();

}
