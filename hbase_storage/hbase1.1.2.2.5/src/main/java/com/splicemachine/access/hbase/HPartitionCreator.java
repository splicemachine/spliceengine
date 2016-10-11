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

package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class HPartitionCreator implements PartitionCreator{
    private HTableDescriptor descriptor;
    private final Connection connection;
    private final HColumnDescriptor userDataFamilyDescriptor;
    private final Clock clock;
    private final HBaseTableInfoFactory tableInfoFactory;
    private final PartitionInfoCache partitionInfoCache;

    public HPartitionCreator(HBaseTableInfoFactory tableInfoFactory,Connection connection,Clock clock,HColumnDescriptor userDataFamilyDescriptor,PartitionInfoCache partitionInfoCache){
        this.connection = connection;
        this.userDataFamilyDescriptor = userDataFamilyDescriptor;
        this.tableInfoFactory = tableInfoFactory;
        this.clock = clock;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public PartitionCreator withName(String name){
        descriptor = new HTableDescriptor(tableInfoFactory.getTableInfo(name));
        return this;
    }

    @Override
    public PartitionCreator withDisplayNames(String[] displayNames){
        descriptor.setValue(SIConstants.TABLE_DISPLAY_NAME_ATTR, displayNames[0] != null ? displayNames[0] : descriptor.getNameAsString());
        descriptor.setValue(SIConstants.INDEX_DISPLAY_NAME_ATTR, displayNames[1]);
        return this;
    }

    @Override
    public PartitionCreator withPartitionSize(long partitionSize){
        descriptor.setMaxFileSize(partitionSize*1024*1024);
        return this;
    }

    @Override
    public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
        assert descriptor!=null: "Programmer error: must specify name first!";
        descriptor.addCoprocessor(coprocessor);
        return this;
    }

    @Override
    public Partition create() throws IOException{
        assert descriptor!=null: "No table to create!";
        descriptor.addFamily(userDataFamilyDescriptor);
        try(Admin admin = connection.getAdmin()){
            admin.createTable(descriptor);
        }
        TableName tableName=descriptor.getTableName();
        return new ClientPartition(connection,tableName,connection.getTable(tableName),clock,partitionInfoCache);
    }
}
