/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
