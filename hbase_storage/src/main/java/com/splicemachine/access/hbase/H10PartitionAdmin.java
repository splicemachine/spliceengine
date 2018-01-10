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
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.util.HBaseFsckRepair;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Function;
import org.apache.hadoop.hbase.*;
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

    private static final Logger LOG = Logger.getLogger(H10PartitionAdmin.class);
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

        try {
            if (splitPoints != null && splitPoints.length > 0) {
                for (byte[] splitPoint : splitPoints) {
                    retriableSplit(tableInfo, splitPoint);
                }
            } else {
                retriableSplit(tableInfo, null);
            }
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void retriableSplit(TableName tableInfo, byte[] splitPoint) throws IOException, SQLException{

        List<HRegionInfo> regions = admin.getTableRegions(tableInfo);
        int size1 = regions.size();
        boolean done = false;
        while (!done) {
            try {
                if (splitPoint != null) {
                    admin.split(tableInfo, splitPoint);
                }
                else {
                    admin.split(tableInfo);
                }
                done = true;
            } catch (NotServingRegionException | ConnectionClosingException e) {
                
                done = false;
                try {
                    timeKeeper.sleep(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    throw new InterruptedIOException();
                }
            } catch (IOException e) {
                String errMsg = e.getMessage();
                if (errMsg.compareTo("should not give a splitkey which equals to startkey!") == 0) {
                    SpliceLogUtils.warn(LOG, errMsg + ": " + Bytes.toStringBinary(splitPoint));
                    SQLWarning warning =
                            StandardException.newWarning(SQLState.SPLITKEY_EQUALS_STARTKEY, Bytes.toStringBinary(splitPoint));
                    EmbedConnection connection = (EmbedConnection)InternalDriver.activeDriver()
                            .connect("jdbc:default:connection", null);
                    Activation lastActivation=connection.getLanguageConnection().getLastActivation();
                    lastActivation.addWarning(warning);
                    return;
                }
                else
                    throw e;
            }
        }

        // Wait until split takes effect or timeout.
        done = false;
        int wait = 0;
        while (!done && wait < 100) {
            regions = admin.getTableRegions(tableInfo);
            int size2 = regions.size();
            if (size2 > size1) {
                done = true;
            }
            else {
                try {
                    timeKeeper.sleep(100, TimeUnit.MILLISECONDS);
                    wait++;
                } catch (InterruptedException ie) {
                    throw new InterruptedIOException();
                }
            }
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
    public void mergeRegions(String regionName1, String regionName2) throws IOException {
        admin.mergeRegions(Bytes.toBytes(regionName1), Bytes.toBytes(regionName2), false);
    }

    @Override
    public void close() throws IOException{
        admin.close();
    }

    @Override
    public Collection<PartitionServer> allServers() throws IOException{
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        return Collections2.transform(servers,new Function<ServerName, PartitionServer>(){
            @Override
            public PartitionServer apply(ServerName input){
                return new HServer(input, clusterStatus);
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
        admin.move(partition.getBytes(), server!=null && !server.isEmpty() ?server.getBytes():null);
    }

    @Override
    public TableDescriptor getTableDescriptor(String table) throws IOException{
        HTableDescriptor hTableDescriptor = admin.getTableDescriptor(TableName.valueOf(table));
        return new HBaseTableDescriptor(hTableDescriptor);
    }

    @Override
    public void snapshot(String snapshotName, String tableName) throws IOException{
        admin.snapshot(snapshotName, TableName.valueOf(tableName));
    }

    @Override
    public void deleteSnapshot(String snapshotName) throws IOException{
       admin.deleteSnapshot(snapshotName);
    }

    @Override
    public void restoreSnapshot(String snapshotName) throws IOException{
        admin.restoreSnapshot(snapshotName);
    }

    @Override
    public void disableTable(String tableName) throws IOException
    {
        admin.disableTable(TableName.valueOf(tableName));
    }

    @Override
    public void enableTable(String tableName) throws IOException
    {
        admin.enableTable(TableName.valueOf(tableName));
    }

    @Override
    public void closeRegion(Partition partition) throws IOException, InterruptedException
    {
        String regionName = partition.getName();
        HRegionInfo regionInfo = ((RangedClientPartition) partition).getRegionInfo();
        ClusterConnection connection = (ClusterConnection) admin.getConnection();
        HRegionLocation regionLocation = MetaTableAccessor.getRegionLocation(connection, Bytes.toBytesBinary(regionName));
        HBaseFsckRepair.closeRegionSilentlyAndWait(connection, regionLocation.getServerName(), regionInfo);

    }

    @Override
    public void assign(Partition partition) throws IOException, InterruptedException
    {
        String regionName = partition.getName();
        admin.assign(regionName.getBytes());
        HBaseFsckRepair.waitUntilAssigned(admin, ((RangedClientPartition)partition).getRegionInfo());

    }

    @Override
    public boolean tableExists(String tableName) throws IOException
    {
        return admin.tableExists(tableInfoFactory.getTableInfo(tableName));
    }
}
