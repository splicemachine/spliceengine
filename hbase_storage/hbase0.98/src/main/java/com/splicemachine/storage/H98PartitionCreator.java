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

package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class H98PartitionCreator implements PartitionCreator{
    private HTableDescriptor descriptor;
    private final HConnection connection;
    private final HColumnDescriptor userDataFamilyDescriptor;

    public H98PartitionCreator(HConnection connection,HColumnDescriptor userDataFamilyDescriptor){
        this.connection = connection;
        this.userDataFamilyDescriptor = userDataFamilyDescriptor;
    }

    @Override
    public PartitionCreator withName(String name){
        descriptor = new HTableDescriptor(HBaseTableInfoFactory.getInstance().getTableInfo(name));
        return this;
    }

    @Override
    public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
        assert descriptor!=null: "Programmer error: must specify name first!";
        descriptor.addCoprocessor(coprocessor);
        return this;
    }

    @Override
    public void create() throws IOException{
        assert descriptor!=null: "No table to create!";
        descriptor.addFamily(userDataFamilyDescriptor);
        try(HBaseAdmin admin = new HBaseAdmin(connection)){
            admin.createTable(descriptor);
        }
    }
}

