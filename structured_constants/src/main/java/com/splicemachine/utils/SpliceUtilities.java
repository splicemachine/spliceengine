package com.splicemachine.utils;

import java.io.IOException;

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;

public class SpliceUtilities extends SIConstants {
	private static final Logger LOG = Logger.getLogger(SpliceUtilities.class);
    private static byte[][] PREFIXES;
	
	public static HBaseAdmin getAdmin() {
		try {
			return new HBaseAdmin(config);
		} catch (MasterNotRunningException e) {
			throw new RuntimeException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	public static Configuration getConfig() {
		return config;
	}
	
	public static HBaseAdmin getAdmin(Configuration configuration) {
		try {
			return new HBaseAdmin(configuration);
		} catch (MasterNotRunningException e) {
			throw new RuntimeException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
            throw new RuntimeException(e);
        }
	}

	

	public static HTableDescriptor generateDefaultSIGovernedTable(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily(createDataFamily());
        //desc.addFamily(createTransactionFamily()); // Removed transaction family
        return desc;
	}
	
	public static HTableDescriptor generateNonSITable(String tableName) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily(createDataFamily());
        return desc;
	}

	public static HTableDescriptor generateTempTable(String tableName) {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		desc.addFamily(createTempDataFamily());
		desc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
		desc.setMaxFileSize(SpliceConstants.tempTableMaxFileSize);
        return desc;
	}

	
    public static HTableDescriptor generateTransactionTable() {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(SpliceConstants.TRANSACTION_TABLE_BYTES));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY.getBytes());
        columnDescriptor.setMaxVersions(5);
        columnDescriptor.setCompressionType(Compression.Algorithm.valueOf(compression.toUpperCase()));
        columnDescriptor.setInMemory(DEFAULT_IN_MEMORY);
        columnDescriptor.setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
        columnDescriptor.setBloomFilterType(BloomType.valueOf(DEFAULT_BLOOMFILTER.toUpperCase()));
        columnDescriptor.setTimeToLive(DEFAULT_TTL);
        desc.addFamily(columnDescriptor);
        desc.addFamily(new HColumnDescriptor(SI_PERMISSION_FAMILY.getBytes()));
        return desc;
    }

    public static byte[][] generateTransactionSplits() {
        byte[][] result = new byte[TRANSACTION_TABLE_BUCKET_COUNT-1][];
        for (int i=0; i<result.length; i++) {
            result[i] = new byte[] {(byte) (i+1)};
        }
        return result;
    }

    public static HColumnDescriptor createDataFamily() {
        HColumnDescriptor snapshot = new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes());
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.valueOf(compression.toUpperCase()));
        snapshot.setInMemory(DEFAULT_IN_MEMORY);
        snapshot.setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
        snapshot.setBloomFilterType(BloomType.ROW);
        snapshot.setTimeToLive(DEFAULT_TTL);
        return snapshot;
	}

    public static HColumnDescriptor createTempDataFamily() {
        HColumnDescriptor snapshot = new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes());
        snapshot.setMaxVersions(Integer.MAX_VALUE);
        snapshot.setCompressionType(Compression.Algorithm.valueOf(compression.toUpperCase()));
        snapshot.setInMemory(DEFAULT_IN_MEMORY);
        snapshot.setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
        //snapshot.setBloomFilterType(BloomType.ROW); No Temp Bloom Filter, write as quickly as possible
        // TODO XXX JLEACH make sure this actually saves us time on the scan side
        snapshot.setTimeToLive(DEFAULT_TTL);
        return snapshot;
	}

    public static void refreshHbase() {
        SpliceLogUtils.info(LOG, "Refresh HBase");
        HBaseAdmin admin = null;
        try{
            admin = getAdmin();
            HTableDescriptor[] descriptors = admin.listTables();
            for (HTableDescriptor desc : descriptors) {
                if (! admin.isTableDisabled(desc.getName())) {
                    admin.disableTable(desc.getName());
                }
                admin.deleteTable(desc.getName());
            }
        }catch(Exception e){
            // TODO: should this be logged and thrown? If we get this exception during startup, we will fail to start.
            SpliceLogUtils.error(LOG,"Unable to Refresh Hbase",e);
        }finally{
            Closeables.closeQuietly(admin);
        }
    }
    
    public static boolean createSpliceHBaseTables () {
        SpliceLogUtils.info(LOG, "Creating Splice Required HBase Tables");
        HBaseAdmin admin = null;
        
        try{
            admin = getAdmin();
            if(!admin.tableExists(TEMP_TABLE_BYTES)){
                createTempTable(admin);
            }
            if(!admin.tableExists(SpliceConstants.TRANSACTION_TABLE_BYTES)){
                HTableDescriptor td = generateTransactionTable();
                admin.createTable(td, generateTransactionSplits());
                SpliceLogUtils.info(LOG, SpliceConstants.TRANSACTION_TABLE_BYTES+" created");
            }
            if(!admin.tableExists(SpliceConstants.TENTATIVE_TABLE_BYTES)){
                HTableDescriptor td = generateDefaultSIGovernedTable(SpliceConstants.TENTATIVE_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.TENTATIVE_TABLE_BYTES+" created");
            }

            if(!admin.tableExists(SpliceConstants.CONGLOMERATE_TABLE_NAME_BYTES)){
                HTableDescriptor td = generateDefaultSIGovernedTable(CONGLOMERATE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.CONGLOMERATE_TABLE_NAME_BYTES+" created");
            }

            /*
             * We have to have a special table to hold our Sequence values, because we shouldn't
             * manage sequential generators transactionally.
             */
            if(!admin.tableExists(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES)){
                HTableDescriptor td = generateNonSITable(SEQUENCE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG,SpliceConstants.SEQUENCE_TABLE_NAME_BYTES.getNameAsString()+" created");
            }

            
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to set up HBase Tables",e);
            return false;
        }finally{
        	Closeables.closeQuietly(admin);
        }
    }

    public static void createTempTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor td = generateTempTable(TEMP_TABLE);
        td.setMaxFileSize(SpliceConstants.tempTableMaxFileSize);
        byte[][] prefixes = getAllPossibleBucketPrefixes();
        byte[][] splitKeys = new byte[prefixes.length - 1][];
        System.arraycopy(prefixes, 1, splitKeys, 0, prefixes.length - 1);
        admin.createTable(td, splitKeys);
        SpliceLogUtils.info(LOG, SpliceConstants.TEMP_TABLE+" created");
    }

    static {
        PREFIXES = new byte[16][];
        for (int i = 0; i < 16; i++) {
            PREFIXES[i] = new byte[] { (byte) ( i * 0x10 ) };
        }
    }

    public static byte[][] getAllPossibleBucketPrefixes() {
        return PREFIXES;
    }

    public static void closeHTableQuietly(HTableInterface table) {
		try {
			if (table != null)
				table.close();
		} catch (Exception e) {
			SpliceLogUtils.error(LOG, e);
		}
	}
    
}
