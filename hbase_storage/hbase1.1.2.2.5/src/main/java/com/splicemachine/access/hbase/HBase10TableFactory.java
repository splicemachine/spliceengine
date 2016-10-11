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
public class HBase10TableFactory implements PartitionFactory<TableName>{
    private Connection connection;
    private Clock timeKeeper;
    private volatile AtomicBoolean initialized = new AtomicBoolean(false);
    private String namespace;
    private byte[] namespaceBytes;
    private HBaseTableInfoFactory tableInfoFactory;
    private PartitionInfoCache<TableName> partitionInfoCache;

    //must be no-args to support the TableFactoryService
    public HBase10TableFactory(){ }

    @Override
    public void initialize(Clock timeKeeper,SConfiguration configuration, PartitionInfoCache<TableName> partitionInfoCache) throws IOException{
        if(!initialized.compareAndSet(false,true))
            return; //already initialized by someone else
        this.partitionInfoCache = partitionInfoCache;
        this.tableInfoFactory = HBaseTableInfoFactory.getInstance(configuration);
        this.timeKeeper = timeKeeper;
        try{
            connection=HBaseConnectionFactory.getInstance(configuration).getConnection();
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
        return new H10PartitionAdmin(connection.getAdmin(),timeKeeper,tableInfoFactory,partitionInfoCache);
    }

    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
