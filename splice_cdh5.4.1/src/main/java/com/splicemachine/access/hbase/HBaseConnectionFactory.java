package com.splicemachine.access.hbase;

import com.google.common.io.Closeables;
import com.splicemachine.access.api.SConnectionFactory;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import static com.splicemachine.si.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.si.constants.SIConstants.SI_PERMISSION_FAMILY;
import static com.splicemachine.si.constants.SIConstants.TRANSACTION_TABLE_BUCKET_COUNT;


/**
 * ConnectionFactory for the HBase architecture.
 *
 * Created by jleach on 11/18/15.
 */
@ThreadSafe
public class HBaseConnectionFactory extends SIConstants implements SConnectionFactory<Connection,Admin> {
    private static final Logger LOG = Logger.getLogger(HBaseConnectionFactory.class);
    private static Connection connection;
    private static HBaseConnectionFactory INSTANCE = new HBaseConnectionFactory();

    private HBaseConnectionFactory() { } //singleton

    public static HBaseConnectionFactory getInstance() {
        return INSTANCE;
    }

    static {
        try {
            connection = ConnectionFactory.createConnection(SpliceConstants.config);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    public Connection getConnection() throws IOException {
        return connection;
    }

    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    /**
     * Returns list of active region server names
     */
    public List<ServerName> getServers() throws SQLException {
        Admin admin = null;
        List<ServerName> servers = null;
        try {
            admin = getAdmin();
            try {
                servers =new ArrayList<>(admin.getClusterStatus().getServers());
            } catch (IOException e) {
                throw new SQLException(e);
            }

        }
        catch (IOException ioe) {
            throw new SQLException(ioe);
        }
        finally {
            if (admin != null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
        }
        return servers;
    }

    /**
     * Returns master server name
     */
    public ServerName getMasterServer() throws SQLException {
        try(Admin admin = getAdmin()){
            try {
                return admin.getClusterStatus().getMaster();
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } catch (IOException ioe) {
            throw new SQLException(ioe);
        }
    }

    public static void deleteTable(HBaseAdmin admin, HTableDescriptor table) throws IOException {
        deleteTable(admin, table.getName());
    }

    public static void deleteTable(HBaseAdmin admin, long conglomerateID) throws IOException {
        deleteTable(admin, Bytes.toBytes(Long.toString(conglomerateID)));
    }

    public static void deleteTable(HBaseAdmin admin, byte[] id) throws IOException {
        admin.disableTable(id);
        admin.deleteTable(id);
    }

    public static HTableDescriptor generateDefaultSIGovernedTable(
            String tableName) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SpliceConstants.spliceNamespace, tableName));
        desc.addFamily(createDataFamily());
        return desc;
    }

    public static HTableDescriptor generateNonSITable(String tableName) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SpliceConstants.spliceNamespace,tableName));
        desc.addFamily(createDataFamily());
        return desc;
    }

    public static HTableDescriptor generateTransactionTable() {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SpliceConstants.spliceNamespaceBytes, TRANSACTION_TABLE_BYTES));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(
                DEFAULT_FAMILY_BYTES);
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setCompressionType(Compression.Algorithm
                .valueOf(compression.toUpperCase()));
        columnDescriptor.setInMemory(DEFAULT_IN_MEMORY);
        columnDescriptor.setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
        columnDescriptor.setBloomFilterType(BloomType
                .valueOf(DEFAULT_BLOOMFILTER.toUpperCase()));
        columnDescriptor.setTimeToLive(DEFAULT_TTL);
        desc.addFamily(columnDescriptor);
        desc.addFamily(new HColumnDescriptor(Bytes.toBytes(SI_PERMISSION_FAMILY)));
        return desc;
    }

    public static byte[][] generateTransactionSplits() {
        byte[][] result = new byte[TRANSACTION_TABLE_BUCKET_COUNT - 1][];
        for (int i = 0; i < result.length; i++) {
            result[i] = new byte[] { (byte) (i + 1) };
        }
        return result;
    }

    public static HColumnDescriptor createDataFamily() {
        HColumnDescriptor snapshot = new HColumnDescriptor(
                DEFAULT_FAMILY_BYTES);
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.valueOf(compression
                .toUpperCase()));
        snapshot.setInMemory(DEFAULT_IN_MEMORY);
        snapshot.setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
        snapshot.setBloomFilterType(BloomType.ROW);
        snapshot.setTimeToLive(DEFAULT_TTL);
        return snapshot;
    }

    public static boolean createSpliceHBaseTables() {
        SpliceLogUtils.info(LOG, "Creating Splice Required HBase Tables");
        Admin admin = null;

        try {
            admin = connection.getAdmin();
            admin.createNamespace(NamespaceDescriptor.create("splice").build());

            if (!admin.tableExists(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,TRANSACTION_TABLE_BYTES))) {
                HTableDescriptor td = generateTransactionTable();
                admin.createTable(td, generateTransactionSplits());
                SpliceLogUtils.info(LOG, TRANSACTION_TABLE
                        + " created");
            }
            if (!admin.tableExists(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,TENTATIVE_TABLE_BYTES))) {
                HTableDescriptor td = generateDefaultSIGovernedTable(TENTATIVE_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, TENTATIVE_TABLE
                        + " created");
            }

            if (!admin
                    .tableExists(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,CONGLOMERATE_TABLE_NAME_BYTES))) {
                HTableDescriptor td = generateDefaultSIGovernedTable(CONGLOMERATE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,
                        CONGLOMERATE_TABLE_NAME + " created");
            }

			/*
			 * We have to have a special table to hold our Sequence values,
			 * because we shouldn't manage sequential generators
			 * transactionally.
			 */
            if (!admin.tableExists(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,SpliceConstants.SEQUENCE_TABLE_NAME_BYTES))) {
                HTableDescriptor td = generateNonSITable(SEQUENCE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SEQUENCE_TABLE_NAME
                        + " created");
            }

            createRestoreTableIfNecessary();
            return true;
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unable to set up HBase Tables", e);
            return false;
        } finally {
            Closeables.closeQuietly(admin);
        }
    }

    public static void createRestoreTableIfNecessary() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(SpliceConstants.spliceNamespaceBytes,RESTORE_TABLE_NAME_BYTES))) {
                HTableDescriptor td = generateNonSITable(RESTORE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, RESTORE_TABLE_NAME
                        + " created");
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Unable to set up HBase Tables", e);
        } finally {
            Closeables.closeQuietly(admin);
        }
    }

    public static String escape(String first) {
        // escape single quotes | compress multiple whitespace chars into one,
        // (replacing tab, newline, etc)
        return first.replaceAll("\\'", "\\'\\'").replaceAll("\\s+", " ");
    }

}
