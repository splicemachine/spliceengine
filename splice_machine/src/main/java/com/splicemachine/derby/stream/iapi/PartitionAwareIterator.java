package com.splicemachine.derby.stream.iapi;


import java.util.Iterator;

/**
 * Created by jleach on 4/28/15.
 */
public interface PartitionAwareIterator<T> extends Iterator<T> {
    byte[] getPartition();
}
