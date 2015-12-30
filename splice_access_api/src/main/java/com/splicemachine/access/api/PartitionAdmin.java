package com.splicemachine.access.api;

import java.io.IOException;
/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public interface PartitionAdmin extends AutoCloseable{

    PartitionCreator newPartition() throws IOException;

    void deleteTable(String tableName) throws IOException;

    void splitTable(String tableName,byte[]... splitPoints) throws IOException;

    void close() throws IOException;
}
