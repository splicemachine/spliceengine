/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

    void splitRegion(byte[] regionName, byte[]... splitPoints) throws IOException;

    void close() throws IOException;

    Collection<PartitionServer> allServers() throws IOException;

    Iterable<? extends Partition> allPartitions(String tableName) throws IOException;

    Iterable<TableDescriptor> listTables() throws IOException;

    TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException;

    void move(String partition,String server) throws IOException;

    TableDescriptor getTableDescriptor(String table) throws IOException;
}
