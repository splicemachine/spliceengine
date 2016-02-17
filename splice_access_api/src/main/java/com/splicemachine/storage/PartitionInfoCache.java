package com.splicemachine.storage;

import com.splicemachine.access.api.SConfiguration;
import java.io.IOException;
import java.util.List;

/**
 * API reference for caching partition information.
 *
 * @author Scott Fines
 *         Date: 12/29/15
 */
public interface PartitionInfoCache<TableInfo>{
    void configure(SConfiguration configuration);
    void invalidate(TableInfo tableInfo) throws IOException;
    List<Partition> getIfPresent(TableInfo tableInfo) throws IOException;
    void put(TableInfo tableInfo, List<Partition> partitions) throws IOException;
}