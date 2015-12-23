package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * A Factory which can create Partitions
 * Created by jleach on 11/18/15.
 */
public interface PartitionFactory<SpliceTableInfo> {

    Partition getTable(SpliceTableInfo tableName) throws IOException;

    Partition getTable(String name) throws IOException;

    Partition getTable(byte[] name) throws IOException;

    void createPartition(String name) throws IOException;

}
