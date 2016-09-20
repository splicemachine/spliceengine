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
