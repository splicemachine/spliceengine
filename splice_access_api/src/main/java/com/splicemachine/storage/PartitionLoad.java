package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface PartitionLoad{
    int getStorefileSizeMB();

    int getMemStoreSizeMB();

    int getStorefileIndexSizeMB();

    String getPartitionName();
}
