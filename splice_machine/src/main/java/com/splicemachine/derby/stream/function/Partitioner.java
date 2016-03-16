package com.splicemachine.derby.stream.function;

/**
 * This interface represent a Partitioner for a datatype T (ExecRow) <br>
 * <br>
 * It partitions a {@link com.splicemachine.derby.stream.iapi.PairDataSet PairDataSet< T, V >} into {@link #numPartitions()}
 * partitions and assigns each T a partition based on {@link #getPartition(Object)} <br>
 * <br>
 * Created by dgomezferro on 2/29/16.
 *
 */
public interface Partitioner<T> {

    void initialize();

    /**
     * @return number of partitions the dataset is partitioned into
     */
    int numPartitions();

    /**
     * Computes the partition a given key is assigned to
     * @param t
     * @return
     */
    int getPartition(T t);
}
