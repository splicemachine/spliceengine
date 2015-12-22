package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface PartitionLocation{
    String partitionName();

    byte[] startKey();

    boolean containsRow(byte[] key);
}
