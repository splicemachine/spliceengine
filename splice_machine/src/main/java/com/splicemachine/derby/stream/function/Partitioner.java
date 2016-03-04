package com.splicemachine.derby.stream.function;

/**
 * Created by dgomezferro on 2/29/16.
 */
public interface Partitioner<T> {

    void initialize();

    int numPartitions();

    int getPartition(T t);
}
