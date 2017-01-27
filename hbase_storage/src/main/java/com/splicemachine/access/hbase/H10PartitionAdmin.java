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

package com.splicemachine.access.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.spark_project.guava.base.Function;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.spark_project.guava.collect.Collections2;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.LazyPartitionServer;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.RangedClientPartition;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class H10PartitionAdmin implements PartitionAdmin{
    private final Admin admin;
    private final Clock timeKeeper;
    private final HBaseTableInfoFactory tableInfoFactory;
    private final PartitionInfoCache<TableName> partitionInfoCache;

    public H10PartitionAdmin(Admin admin,
                             Clock timeKeeper,
                             HBaseTableInfoFactory tableInfoFactory,
                             PartitionInfoCache<TableName> partitionInfoCache
                             ){
        this.admin=admin;
        this.timeKeeper=timeKeeper;
        this.tableInfoFactory = tableInfoFactory;
        this.partitionInfoCache = partitionInfoCache;
    }

    @Override
    public PartitionCreator newPartition() throws IOException{
        HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(SIDriver.driver().getConfiguration());
        HColumnDescriptor dataFamily = instance.createDataFamily();
        return new HPartitionCreator(tableInfoFactory,admin.getConnection(),timeKeeper,dataFamily,partitionInfoCache);
    }

    @Override
    public void deleteTable(String tableName) throws IOException{
        try{
            admin.disableTable(tableInfoFactory.getTableInfo(tableName));
            admin.deleteTable(tableInfoFactory.getTableInfo(tableName));
        }catch(TableNotFoundException ignored){
            /*
             * If the table isn't there, then we probably have two concurrent Vacuums going on. In that
             * situation, we just ignore the error, since the delete already works.
             */
        }
    }

    /**
     * Split a table. Asynchronous operation.
     *
     * @param tableName table to split
     * @param splitPoints the explicit positions to split on. If null, HBase chooses.
     * @throws IOException if a remote or network exception occurs
     */
    @Override
    public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
        TableName tableInfo=tableInfoFactory.getTableInfo(tableName);
        if (splitPoints != null && splitPoints.length > 0) {
            for(byte[] splitPoint:splitPoints){
                admin.split(tableInfo,splitPoint);
            }
        } else {
            admin.split(tableInfo);
        }
    }

    @Override
    public void splitRegion(byte[] regionName, byte[]... splitPoints) throws IOException {
        if (splitPoints != null && splitPoints.length > 0) {
            for(byte[] splitPoint:splitPoints){
                admin.splitRegion(regionName, splitPoint);
            }
        } else {
            admin.splitRegion(regionName);
        }
    }

    @Override
    public void close() throws IOException{
        admin.close();
    }

    @Override
    public Collection<PartitionServer> allServers() throws IOException{
        Collection<ServerName> servers=admin.getClusterStatus().getServers();
        return Collections2.transform(servers,new Function<ServerName, PartitionServer>(){
            @Override
            public PartitionServer apply(ServerName input){
                return new HServer(input,admin);
            }
        });
    }

    @Override
    public Iterable<? extends Partition> allPartitions(String tableName) throws IOException{
        TableName tn =tableInfoFactory.getTableInfo(tableName);
        List<HRegionInfo> tableRegions=admin.getTableRegions(tn);
        List<Partition> partitions=new ArrayList<>(tableRegions.size());
        Connection connection=admin.getConnection();
        Table table = connection.getTable(tn);
        for(HRegionInfo info : tableRegions){
            LazyPartitionServer owningServer=new LazyPartitionServer(connection,info,tn);
            partitions.add(new RangedClientPartition(connection,tn,table,info,owningServer,timeKeeper,partitionInfoCache));
        }
        return partitions;
    }

    @Override
    public TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException{
        HTableDescriptor[] hTableDescriptors = admin.getTableDescriptors(tables);
        TableDescriptor[] tableDescriptors = new TableDescriptor[hTableDescriptors.length];
        for (int i = 0; i < hTableDescriptors.length; ++i) {
            tableDescriptors[i] = new HBaseTableDescriptor(hTableDescriptors[i]);
        }
        return tableDescriptors;
    }

    @Override
    public Iterable<TableDescriptor> listTables() throws IOException {

        HTableDescriptor[] hTableDescriptors = admin.listTables();
        TableDescriptor[] tableDescriptors = new TableDescriptor[hTableDescriptors.length];
        for (int i = 0; i < hTableDescriptors.length; ++i) {
            tableDescriptors[i] = new HBaseTableDescriptor(hTableDescriptors[i]);
        }
        return Arrays.asList(tableDescriptors);
    }

    @Override
    public void move(String partition, String server) throws IOException {
        admin.move(partition.getBytes(), server!=null && server.length()>0?server.getBytes():null);
    }

    @Override
    public TableDescriptor getTableDescriptor(String table) throws IOException{
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf(table));
        return new HBaseTableDescriptor(hTableDescriptor);
    }
}
