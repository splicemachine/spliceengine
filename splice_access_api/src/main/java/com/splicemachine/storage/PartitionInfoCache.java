package com.splicemachine.storage;

import java.io.IOException;

/**
 * API reference for caching partition information.
 *
 * @author Scott Fines
 *         Date: 12/29/15
 */
public interface PartitionInfoCache{
    void invalidate(String tableName) throws IOException;
    void invalidate(byte[] tableNameBytes) throws IOException;
}
