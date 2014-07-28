package com.splicemachine.derby.utils;

/**
 * Created by jyuan on 7/27/14.
 */
public interface PartitionAwareIterator<T> extends StandardIterator<T> {

    byte[] getPartition();
}
