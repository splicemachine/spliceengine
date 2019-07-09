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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
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

        TableName tn = tableInfoFactory.getTableInfo(tableName);
        try{
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
        }catch(TableNotFoundException ignored){
            /*
             * If the table isn't there, then we probably have two concurrent Vacuums going on. In that
             * situation, we just ignore the error, since the delete already works.
             */
            SpliceLogUtils.warn(LOG, "Table %s not found. It may be deleted by another running vacuum.", tableName);
        }catch (Exception e) {
            SpliceLogUtils.warn(LOG, "Deleting table %s failed", tableName);
            if (!admin.isTableEnabled(tn)){
                SpliceLogUtils.warn(LOG, "Re-enable table %s", tableName);
                admin.enableTable(tn);
            }
            throw e;
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
                    SpliceLogUtils.warn(LOG, "%s : %s", errMsg, Bytes.toStringBinary(splitPoint));
                    SQLWarning warning =
                            StandardException.newWarning(SQLState.SPLITKEY_EQUALS_STARTKEY, Bytes.toStringBinary(splitPoint));
                    try {
                        InternalDriver internalDriver = InternalDriver.activeDriver();
                        if (internalDriver != null) {
                            EmbedConnection connection = (EmbedConnection)internalDriver.connect("jdbc:default:connection", null);
                            if (connection != null) {
                                LanguageConnectionContext lcc = connection.getLanguageConnection();
                                if (lcc != null) {
                                    Activation lastActivation = lcc.getLastActivation();
                                    lastActivation.addWarning(warning);
                                }
                            }
                        }
                    } catch (Exception ex) {
                        // ignore any error when adding a warning to activation, because the warning is written
                        // to log.
                    }
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
        retriableMergeRegions(regionName1, regionName2, 100, 0);
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
            ClientPartition clientPartition = new ClientPartition(connection, table.getName(), table, timeKeeper, partitionInfoCache);
            partitions.add(new RangedClientPartition(clientPartition,info,owningServer));
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

    @Override
    public void markDropped(long conglomId, long txn) throws IOException {
        Table dropped = admin.getConnection().getTable(tableInfoFactory.getTableInfo(HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME));
        Put put = new Put(Bytes.toBytes(conglomId));
        put.addImmutable(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, Bytes.toBytes(txn));
        dropped.put(put);
    }

    @Override
    public List<byte[]> hbaseOperation(String tableName, String operation, byte[] bytes) throws IOException {
        try {
            switch (operation) {
                case "get": {
                    ClientProtos.Get getRequest = ClientProtos.Get.parseFrom(bytes);
                    Get get = ProtobufUtil.toGet(getRequest);
                    try (Table table = admin.getConnection().getTable(TableName.valueOf(tableName))) {
                        Result result = table.get(get);
                        ClientProtos.Result pbr = ProtobufUtil.toResult(result);
                        return Arrays.asList(pbr.toByteArray());
                    }
                }
                case "batchGet": {
                    InputStream is = new ByteArrayInputStream(bytes);
                    List<Get> gets = new ArrayList<>();
                    while (is.available() > 0) {
                        ClientProtos.Get getRequest = ClientProtos.Get.parseDelimitedFrom(is);
                        gets.add(ProtobufUtil.toGet(getRequest));
                    }
                    is.close();

                    List<byte[]> results = new ArrayList<>(gets.size());
                    try (Table table = admin.getConnection().getTable(TableName.valueOf(tableName))) {
                        Result[] result = table.get(gets);
                        for (Result r : result) {
                            ClientProtos.Result pbr = ProtobufUtil.toResult(r);
                            results.add(pbr.toByteArray());
                        }
                    }
                    return results;
                }
                case "scan":
                    ClientProtos.Scan scanRequest = ClientProtos.Scan.parseFrom(bytes);
                    Scan scan = ProtobufUtil.toScan(scanRequest);
                    try (Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
                         ResultScanner result = table.getScanner(scan)) {
                        List<byte[]> results = new ArrayList<>();
                        for (Result r : result) {
                            ClientProtos.Result pbr = ProtobufUtil.toResult(r);
                            results.add(pbr.toByteArray());
                        }
                        return results;
                    }
                case "descriptor":
                    try (Table table = admin.getConnection().getTable(TableName.valueOf(tableName))) {
                        HTableDescriptor descriptor = table.getTableDescriptor();
                        return Arrays.asList(descriptor.convert().toByteArray());
                    }
                case "grant":
                    String userName = Bytes.toString(bytes);
                    String spliceNamespace = SIDriver.driver().getConfiguration().getNamespace();
                    grantPrivilegesIfNeeded(userName, spliceNamespace);

                    return Collections.emptyList();

                case "grantCreatePrivilege": {
                    userName = Bytes.toString(bytes);
                    boolean granted = grantCreatePrivilege(tableName, userName);
                    return Arrays.asList(new Boolean(granted).toString().getBytes());
                }

                case "revokeCreatePrivilege": {
                    userName = Bytes.toString(bytes);
                    boolean granted = revokeCreatePrivilege(tableName, userName);
                    return Arrays.asList(new Boolean(granted).toString().getBytes());
                }
                default:
                    throw new UnsupportedOperationException(operation);

            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
            throw new IOException(e);
        } catch (Throwable t) {
            throw new IOException(t);
        }
    }

    @Override
    public void enableTableReplication(String tableName) throws IOException {
        TableName tn = tableInfoFactory.getTableInfo(tableName);
        boolean tableEnabled = admin.isTableEnabled(tn);
        try {
            HTableDescriptor htd = admin.getTableDescriptor(tn);
            if (tableEnabled) {
                admin.disableTable(tn);
            }

            for (HColumnDescriptor hcd : htd.getFamilies()) {
                hcd.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
            }
            admin.modifyTable(tn, htd);
            SpliceLogUtils.info(LOG, "enabled replication for table %s", tn);
        }
        finally {
            if (tn != null && !admin.isTableEnabled(tn) && tableEnabled) {
                admin.enableTable(tn);
            }
        }
    }

    @Override
    public void disableTableReplication(String tableName) throws IOException {
        TableName tn = tableInfoFactory.getTableInfo(tableName);
        boolean tableEnabled = admin.isTableEnabled(tn);
        try {
            HTableDescriptor htd = admin.getTableDescriptor(tn);
            if (tableEnabled) {
                admin.disableTable(tn);
            }

            for (HColumnDescriptor hcd : htd.getFamilies()) {
                hcd.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
            }
            admin.modifyTable(tn, htd);
            SpliceLogUtils.info(LOG, "disabled replication for table %s", tn);
        } finally {
            if (tn != null && !admin.isTableEnabled(tn) && tableEnabled) {
                admin.enableTable(tn);
            }
        }
    }

    private boolean grantCreatePrivilege(String tableName, String userName) throws Throwable{

        if (hasCreatePrivilege(tableName, userName)){
            SpliceLogUtils.info(LOG, "User %s already has create privilege for table %s. Ignore grant request.",
                    userName, tableName);
            return false;
        }

        SpliceLogUtils.info(LOG, "granting create privilege to user %s on table %s", userName, tableName);
        for (String user : Arrays.asList(userName, userName.toUpperCase(), userName.toLowerCase())) {
            AccessControlClient.grant(admin.getConnection(), TableName.valueOf(tableName), user,null, null,
                    Permission.Action.CREATE);
        }
        return true;
    }

    private boolean hasCreatePrivilege(String tableName, String userName) throws Throwable{
        List<UserPermission> permissions = AccessControlClient.getUserPermissions(admin.getConnection(), tableName);
        for (String user : Arrays.asList(userName, userName.toUpperCase(), userName.toLowerCase())) {
            UserPermission up = getPermission(permissions, user);
            if (up == null || !up.implies(TableName.valueOf(tableName), null, null, Permission.Action.CREATE))
                return false;
        }
        return true;
    }


    private boolean revokeCreatePrivilege(String tableName, String userName) throws Throwable{

        if (!hasCreatePrivilege(tableName, userName)){
            SpliceLogUtils.info(LOG, "User %s does not have create privilege for table %s. Ignore revoke request.",
                    userName, tableName);
            return false;
        }

        SpliceLogUtils.info(LOG, "revoking create privilege on table %s from user %s", tableName, userName);
        for (String user : Arrays.asList(userName, userName.toUpperCase(), userName.toLowerCase())) {
            AccessControlClient.revoke(admin.getConnection(), TableName.valueOf(tableName), user,null, null,
                    Permission.Action.CREATE);
        }
        return true;
    }

    private void grantPrivilegesIfNeeded(String userName, String spliceNamespace) throws Throwable {
        if (hasPrivileges(userName, spliceNamespace)) {
            LOG.info("User " + userName + " already has privileges on namespace " + spliceNamespace);
            return;
        }
        
        LOG.info("User " + userName + " lacks some privileges on namespace " + spliceNamespace + ", granting them");

        for (String user : Arrays.asList(userName, userName.toUpperCase(), userName.toLowerCase())) {
            AccessControlClient.grant(admin.getConnection(), spliceNamespace, user,
                    Permission.Action.WRITE
                    , Permission.Action.READ
                    , Permission.Action.EXEC
            );
        }
    }

    private boolean hasPrivileges(String userName, String spliceNamespace) throws Throwable {
        List<UserPermission> permissions = AccessControlClient.getUserPermissions(admin.getConnection(), "@"+spliceNamespace);
        for (String user : Arrays.asList(userName, userName.toUpperCase(), userName.toLowerCase())) {
            UserPermission up = getPermission(permissions, user);
            if (up == null)
                return false;
            
            for (Permission.Action action : Arrays.asList(Permission.Action.WRITE, Permission.Action.READ, Permission.Action.EXEC)) {
                if (!up.implies(spliceNamespace, action))
                    return false;
            }
        }
        return true;
    }

    private UserPermission getPermission(List<UserPermission> permissions, String userName) {
        for(UserPermission up: permissions) {
            if (Bytes.equals(up.getUser(), Bytes.toBytes(userName))) {
                return up;
            }
        }
        return null;
    }

    private void retriableMergeRegions(String regionName1, String regionName2, int maxRetries, int retry) throws IOException {
        try {
            admin.mergeRegions(Bytes.toBytes(regionName1), Bytes.toBytes(regionName2), false);
        }
        catch (MergeRegionException e) {
            SpliceLogUtils.warn(LOG, "Merge failed:", e);
            if (e.getMessage().contains("Unable to merge regions not online") && retry < maxRetries) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    throw new IOException(e);
                }
                SpliceLogUtils.info(LOG, "retry merging region %s and %s", regionName1, regionName2);
                retriableMergeRegions(regionName1, regionName2, maxRetries, retry+1);
            }
            else
                throw e;
        }
    }
}
