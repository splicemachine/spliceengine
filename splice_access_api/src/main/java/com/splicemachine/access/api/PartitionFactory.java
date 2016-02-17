package com.splicemachine.access.api;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;

import java.io.IOException;

/**
 * A Factory which can create Partitions
 * Created by jleach on 11/18/15.
 */
public interface PartitionFactory<SpliceTableInfo>{

    void initialize(Clock clock,SConfiguration configuration, PartitionInfoCache<SpliceTableInfo> partitionInfoCache) throws IOException;

    Partition getTable(SpliceTableInfo tableName) throws IOException;

    Partition getTable(String name) throws IOException;

    Partition getTable(byte[] name) throws IOException;

    PartitionAdmin getAdmin() throws IOException;
}
