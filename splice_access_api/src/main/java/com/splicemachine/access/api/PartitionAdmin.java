package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public interface PartitionAdmin extends AutoCloseable{

    PartitionCreator newPartition() throws IOException;

    void deleteTable(String tableName) throws IOException;

    void splitTable(String tableName,byte[]... splitPoints) throws IOException;

    void close() throws IOException;

    Collection<PartitionServer> allServers() throws IOException;

    Iterable<? extends Partition> allPartitions(String tableName) throws IOException;

    Iterable<TableDescriptor> listTables() throws IOException;

    TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException;
}
