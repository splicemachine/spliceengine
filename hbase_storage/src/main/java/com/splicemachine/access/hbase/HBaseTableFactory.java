/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;

/**
 *
 * Created by jleach on 11/18/15.
 */
public class HBaseTableFactory implements PartitionFactory<TableName>{
    private Connection connection;
    private Clock timeKeeper;
    private volatile AtomicBoolean initialized = new AtomicBoolean(false);
    private String namespace;
    private byte[] namespaceBytes;
    private HBaseTableInfoFactory tableInfoFactory;
    private PartitionInfoCache<TableName> partitionInfoCache;

    //must be no-args to support the TableFactoryService
    public HBaseTableFactory(){ }

    @Override
    public void initialize(Clock timeKeeper,SConfiguration configuration, PartitionInfoCache<TableName> partitionInfoCache) throws IOException{
        if(!initialized.compareAndSet(false,true))
            return; //already initialized by someone else
        this.partitionInfoCache = partitionInfoCache;
        this.tableInfoFactory = HBaseTableInfoFactory.getInstance(configuration);
        this.timeKeeper = timeKeeper;
        try{
            connection= HBaseConnectionFactory.getInstance(configuration).getConnection();
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
        this.namespace = configuration.getNamespace();
        this.namespaceBytes =Bytes.toBytes(namespace);

    }

    @Override
    public Partition getTable(TableName tableName) throws IOException{
        return new ClientPartition(connection,tableName,connection.getTable(tableName),timeKeeper,partitionInfoCache);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        return getTable(TableName.valueOf(namespace,name));
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(TableName.valueOf(namespaceBytes,name));
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return new HBasePartitionAdmin(connection.getAdmin(),timeKeeper,tableInfoFactory,partitionInfoCache);
    }

    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
