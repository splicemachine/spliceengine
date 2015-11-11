package com.splicemachine.utils;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class SpliceUtilities extends SIConstants {
	private static final Logger LOG = Logger.getLogger(SpliceUtilities.class);

	public static HBaseAdmin getAdmin() {
		try {
			return new HBaseAdmin(config);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns list of active region server names
	 */
	public static List<ServerName> getServers() throws SQLException {
		HBaseAdmin admin = null;
		List<ServerName> servers = null;
		try {
			admin = getAdmin();
			try {
				servers = new ArrayList<ServerName>(admin.getClusterStatus()
						.getServers());
			} catch (IOException e) {
				throw new SQLException(e);
			}
		} finally {
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
	public static ServerName getMasterServer() throws SQLException {
		HBaseAdmin admin = null;
		ServerName server = null;
		try {
			admin = getAdmin();
			try {
				server = admin.getClusterStatus().getMaster();
			} catch (IOException e) {
				throw new SQLException(e);
			}
		} finally {
			if (admin != null)
				try {
					admin.close();
				} catch (IOException e) {
					// ignore
				}
		}
		return server;
	}

	public static Configuration getConfig() {
		return config;
	}

	public static HBaseAdmin getAdmin(Configuration configuration) {
		try {
			return new HBaseAdmin(configuration);
		} catch (Exception e) {
			throw new RuntimeException(e);
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
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily(createDataFamily());
		return desc;
	}

	public static HTableDescriptor generateNonSITable(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily(createDataFamily());
		return desc;
	}

	public static HTableDescriptor generateTransactionTable() {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SpliceConstants.TRANSACTION_TABLE_BYTES));
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
				SpliceConstants.DEFAULT_FAMILY_BYTES);
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
		HBaseAdmin admin = null;

		try {
			admin = getAdmin();
			if (!admin.tableExists(SpliceConstants.TRANSACTION_TABLE_BYTES)) {
				HTableDescriptor td = generateTransactionTable();
				admin.createTable(td, generateTransactionSplits());
				SpliceLogUtils.info(LOG, SpliceConstants.TRANSACTION_TABLE
						+ " created");
			}
			if (!admin.tableExists(SpliceConstants.TENTATIVE_TABLE_BYTES)) {
				HTableDescriptor td = generateDefaultSIGovernedTable(SpliceConstants.TENTATIVE_TABLE);
				admin.createTable(td);
				SpliceLogUtils.info(LOG, SpliceConstants.TENTATIVE_TABLE
						+ " created");
			}

			if (!admin
					.tableExists(SpliceConstants.CONGLOMERATE_TABLE_NAME_BYTES)) {
				HTableDescriptor td = generateDefaultSIGovernedTable(CONGLOMERATE_TABLE_NAME);
				admin.createTable(td);
				SpliceLogUtils.info(LOG,
						SpliceConstants.CONGLOMERATE_TABLE_NAME + " created");
			}

			/*
			 * We have to have a special table to hold our Sequence values,
			 * because we shouldn't manage sequential generators
			 * transactionally.
			 */
			if (!admin.tableExists(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES)) {
				HTableDescriptor td = generateNonSITable(SEQUENCE_TABLE_NAME);
				admin.createTable(td);
				SpliceLogUtils.info(LOG, SpliceConstants.SEQUENCE_TABLE_NAME
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
        HBaseAdmin admin = null;
        try {
            admin = getAdmin();
            if (!admin.tableExists(SpliceConstants.RESTORE_TABLE_NAME_BYTES)) {
                HTableDescriptor td = generateNonSITable(RESTORE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.RESTORE_TABLE_NAME
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
