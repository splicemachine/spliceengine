package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PartitionCreator{

    PartitionCreator withName(String name);

    PartitionCreator withCoprocessor(String coprocessor) throws IOException;

    Partition create() throws IOException;
}
