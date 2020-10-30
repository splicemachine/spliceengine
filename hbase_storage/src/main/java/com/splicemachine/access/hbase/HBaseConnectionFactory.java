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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.splicemachine.si.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.si.constants.SIConstants.SI_PERMISSION_FAMILY;
import static com.splicemachine.si.constants.SIConstants.TRANSACTION_TABLE_BUCKET_COUNT;

/**
 * ConnectionFactory for the HBase architecture.
 * <p/>
 * Created by jleach on 11/18/15.
 */
@ThreadSafe
public class HBaseConnectionFactory{
    private static final Logger LOG=Logger.getLogger(HBaseConnectionFactory.class);
    private static volatile HBaseConnectionFactory INSTANCE;
    private final Connection connection;
    private final Connection noRetryConnection;
    private final SConfiguration config;
    private final String namespace;
    private final byte[] namespaceBytes;

    private HBaseConnectionFactory(SConfiguration configuration){
        this.config=configuration;
        this.namespace=configuration.getNamespace();
        this.namespaceBytes= Bytes.toBytes(namespace);
        try{
            Configuration config = (Configuration) configuration.getConfigSource().unwrapDelegate();
            this.connection= ConnectionFactory.createConnection(config);
            Configuration clonedConfig = new Configuration(config);
            clonedConfig.setInt("hbase.client.retries.number",0);
            this.noRetryConnection=ConnectionFactory.createConnection(clonedConfig);
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
    }

    public static HBaseConnectionFactory getInstance(SConfiguration config){
        HBaseConnectionFactory hbcf = INSTANCE;
        if(hbcf==null){
            synchronized(HBaseConnectionFactory.class){
                hbcf = INSTANCE;
                if(hbcf==null)
                    hbcf = INSTANCE = new HBaseConnectionFactory(config);
            }
        }
        return hbcf;

    }


    public Connection getConnection() throws IOException{
        return connection;
    }

    public Connection getNoRetryConnection() throws IOException{
        return noRetryConnection;
    }

    public Admin getAdmin() throws IOException{
        return connection.getAdmin();
    }

    /**
     * Returns list of active region server names
     */
    public List<ServerName> getServers() throws SQLException {
        Admin admin=null;
        List<ServerName> servers=null;
        try{
            admin=getAdmin();
            try{
                servers=new ArrayList<>(admin.getClusterStatus().getServers());
            }catch(IOException e){
                throw new SQLException(e);
            }

        }catch(IOException ioe){
            throw new SQLException(ioe);
        }finally{
            if(admin!=null)
                try{
                    admin.close();
                }catch(IOException e){
                    // ignore
                }
        }
        return servers;
    }

    /**
     * Returns master server name
     */
    public ServerName getMasterServer() throws SQLException{
        try(Admin admin=getAdmin()){
            try{
                return admin.getClusterStatus().getMaster();
            }catch(IOException e){
                throw new SQLException(e);
            }
        }catch(IOException ioe){
            throw new SQLException(ioe);
        }
    }

    //    TODO public static void deleteTable(HBaseAdmin admin,HTableDescriptor table) throws IOException{
    //        deleteTable(admin,table.getTableName().getName());
    //    }
    //
    //    public static void deleteTable(HBaseAdmin admin,long conglomerateID) throws IOException{
    //        deleteTable(admin,Bytes.toBytes(Long.toString(conglomerateID)));
    //    }
    //
    //    public static void deleteTable(HBaseAdmin admin,byte[] id) throws IOException{
    //        admin.disableTable(id);
    //        admin.deleteTable(id);
    //    }

    public TableDescriptor generateDefaultSIGovernedTable(String tableName){
        TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespace,tableName))
                .setPriority(HBaseTableDescriptor.HIGH_TABLE_PRIORITY)
                .setColumnFamily(createDataFamily())
                .setValue(SIConstants.CATALOG_VERSION_ATTR, HBaseConfiguration.catalogVersions.get(tableName))
                .build();
        return descriptor;
    }

    public TableDescriptor generateNonSITable(String tableName){
        TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespace,tableName))
                .setPriority(HBaseTableDescriptor.HIGH_TABLE_PRIORITY)
                .setColumnFamily(createDataFamily())
                .setValue(SIConstants.CATALOG_VERSION_ATTR, HBaseConfiguration.catalogVersions.get(tableName))
                .build();
        return descriptor;
    }

    public TableDescriptor generateTransactionTable(){
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnDescriptor =
                new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(DEFAULT_FAMILY_BYTES);
        columnDescriptor.setMaxVersions(5);
        Compression.Algorithm compress=Compression.getCompressionAlgorithmByName(config.getCompressionAlgorithm());
        columnDescriptor.setCompressionType(compress);
        columnDescriptor.setInMemory(HConfiguration.DEFAULT_IN_MEMORY);
        columnDescriptor.setBlockCacheEnabled(HConfiguration.DEFAULT_BLOCKCACHE);
        columnDescriptor.setBloomFilterType(BloomType.valueOf(HConfiguration.DEFAULT_BLOOMFILTER.toUpperCase()));
        columnDescriptor.setTimeToLive(HConfiguration.DEFAULT_TTL);
        TableDescriptor descriptor = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespaceBytes, HConfiguration.TRANSACTION_TABLE_BYTES))
                .setPriority(HBaseTableDescriptor.HIGH_TABLE_PRIORITY)
                .setColumnFamily(columnDescriptor)
                .setColumnFamily(new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes(SI_PERMISSION_FAMILY)))
                .build();
        return descriptor;
    }

    public static byte[][] generateTransactionSplits(){
        byte[][] result=new byte[TRANSACTION_TABLE_BUCKET_COUNT-1][];
        for(int i=0;i<result.length;i++){
            result[i]=new byte[]{(byte)(i+1)};
        }
        return result;
    }

    public ColumnFamilyDescriptor createDataFamily(){
        ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor snapshot =
                new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(DEFAULT_FAMILY_BYTES);
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        Compression.Algorithm compress=Compression.getCompressionAlgorithmByName(config.getCompressionAlgorithm());
        snapshot.setCompressionType(compress);
        snapshot.setInMemory(HConfiguration.DEFAULT_IN_MEMORY);
        snapshot.setBlockCacheEnabled(HConfiguration.DEFAULT_BLOCKCACHE);
        snapshot.setBloomFilterType(BloomType.ROW);
        snapshot.setTimeToLive(HConfiguration.DEFAULT_TTL);
        return snapshot;
    }

    public boolean createSpliceHBaseTables(){
        SpliceLogUtils.info(LOG,"Creating Splice Required HBase Tables");

        try(Admin admin=connection.getAdmin()){
            admin.createNamespace(NamespaceDescriptor.create("splice").build());

            if(!admin.tableExists(TableName.valueOf(namespace,HConfiguration.TRANSACTION_TABLE))){
                TableDescriptor td=generateTransactionTable();
                admin.createTable(td,generateTransactionSplits());
                SpliceLogUtils.info(LOG,HConfiguration.TRANSACTION_TABLE+" created");
            }

            if(!admin.tableExists(TableName.valueOf(namespace, HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME))){
                TableDescriptor td=generateDefaultSIGovernedTable(HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME+" created");
            }

            if(!admin.tableExists(TableName.valueOf(namespaceBytes, HBaseConfiguration.CONGLOMERATE_TABLE_NAME_BYTES))){
                TableDescriptor td=generateDefaultSIGovernedTable(HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,HBaseConfiguration.CONGLOMERATE_TABLE_NAME+" created");
            }

                /*
                 * We have to have a special table to hold our Sequence values,
                 * because we shouldn't manage sequential generators
                 * transactionally.
                 */
            if(!admin.tableExists(TableName.valueOf(namespace,HConfiguration.SEQUENCE_TABLE_NAME))){
                TableDescriptor td=generateNonSITable(HConfiguration.SEQUENCE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, HBaseConfiguration.SEQUENCE_TABLE_NAME+" created");
            }

            if(!admin.tableExists(TableName.valueOf(namespace, HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME))){
                TableDescriptor td=generateNonSITable(HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,
                        HConfiguration.MASTER_SNAPSHOTS_TABLE_NAME + " created");
            }

            if(!admin.tableExists(TableName.valueOf(namespace, HConfiguration.REPLICA_REPLICATION_PROGRESS_TABLE_NAME))){
                TableDescriptor td=generateNonSITable(HConfiguration.REPLICA_REPLICATION_PROGRESS_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,
                        HConfiguration.REPLICA_REPLICATION_PROGRESS_TABLE_NAME + " created");
            }

            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to set up HBase Tables",e);
            return false;
        }
    }

    public void createRestoreTable() {

        try(Admin admin=connection.getAdmin()){
            TableDescriptor td=generateNonSITable(HConfiguration.IGNORE_TXN_TABLE_NAME);
            admin.createTable(td);
            SpliceLogUtils.info(LOG, HConfiguration.IGNORE_TXN_TABLE_NAME +" created");
        }catch(Exception e) {
            SpliceLogUtils.error(LOG, "Unable to create SPLICE_IGNORE_TXN Table", e);
        }
    }

    public static String escape(String first){
        // escape single quotes | compress multiple whitespace chars into one,
        // (replacing tab, newline, etc)
        return first.replaceAll("\\'","\\'\\'").replaceAll("\\s+"," ");
    }

}
