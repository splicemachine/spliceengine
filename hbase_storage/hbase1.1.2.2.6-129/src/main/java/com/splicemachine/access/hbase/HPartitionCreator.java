/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
    private byte[][] splitKeys;

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
    public PartitionCreator withTransactionId(long txnId) throws IOException {
        descriptor.setValue(SIConstants.TRANSACTION_ID_ATTR, Long.toString(txnId));
        return this;
    }

    @Override
    public Partition create() throws IOException{
        assert descriptor!=null: "No table to create!";
        descriptor.addFamily(userDataFamilyDescriptor);
        try(Admin admin = connection.getAdmin()){
            if (splitKeys == null) {
                admin.createTable(descriptor);
            }
            else {
                admin.createTable(descriptor, splitKeys);
            }
        }
        TableName tableName=descriptor.getTableName();
        return new ClientPartition(connection,tableName,connection.getTable(tableName),clock,partitionInfoCache);
    }

    @Override
    public PartitionCreator withSplitKeys(byte[][] splitKeys) {
        this.splitKeys = splitKeys;
        return this;
    }
}
