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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MEnginePartitionAdmin implements PartitionAdmin{
    private final PartitionAdmin admin;

    public MEnginePartitionAdmin(PartitionAdmin admin){
        this.admin=admin;
    }

    @Override
    public PartitionCreator newPartition() throws IOException{
        return new PipelinePartitionCreator(admin.newPartition());
    }

    @Override
    public void deleteTable(String tableName) throws IOException{
        admin.deleteTable(tableName);
    }

    @Override
    public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
        admin.splitTable(tableName, splitPoints);
    }

    @Override
    public void splitRegion(byte[] regionName, byte[]... splitPoints) throws IOException {
        admin.splitRegion(regionName, splitPoints);
    }

    @Override
    public void close() throws IOException{
        admin.close();
    }

    @Override
    public Collection<PartitionServer> allServers() throws IOException{
        return admin.allServers();
    }

    @Override
    public Iterable<? extends Partition> allPartitions(String tableName) throws IOException{
        return admin.allPartitions(tableName);
    }

    @Override
    public TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException{
        return admin.getTableDescriptors(tables);
    }

    @Override
    public Iterable<TableDescriptor> listTables() throws IOException {
        return admin.listTables();
    }

    @Override
    public void move(String partition, String server) throws IOException {
        admin.move(partition, server);
    }

    @Override
    public TableDescriptor getTableDescriptor(String table) throws IOException{
        return admin.getTableDescriptor(table);
    }
}
