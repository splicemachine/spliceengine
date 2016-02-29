package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PartitionCreator{

    PartitionCreator withName(String name);

    PartitionCreator withDisplayNames(String[] displayNames);

    /**
     * Set the maximum size of a given subpartition for this overall partition,
     * if the underlying architecture supports table-specific partition sizes.
     *
     * If the architecture does not support table-specific partition sizes, then
     * this is a no-op.
     *
     * @param partitionSize the size of a partition
     * @return a creator
     */
    PartitionCreator withPartitionSize(long partitionSize);

    PartitionCreator withCoprocessor(String coprocessor) throws IOException;

    Partition create() throws IOException;
}
