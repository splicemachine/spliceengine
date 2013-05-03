package com.splicemachine.utils;

import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;

public class SpliceUtilities extends SIConstants {
	private static final Logger LOG = Logger.getLogger(SpliceUtilities.class);
	
	public static HBaseAdmin getAdmin() {
		try {
			return new HBaseAdmin(config);
		} catch (MasterNotRunningException e) {
			throw new RuntimeException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	public static HTableDescriptor generateDefaultSIGovernedTable(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(createDataFamily());
        desc.addFamily(createTransactionFamily());
        return desc;
	}
	
	public static HTableDescriptor generateNonSITable(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(createDataFamily());
        return desc;
	}
	
	public static HTableDescriptor generateTransactionTable(String tableName) {
            HTableDescriptor desc = new HTableDescriptor(SpliceConstants.TRANSACTION_TABLE_BYTES);
            desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY.getBytes(),
                    5,
                    DEFAULT_COMPRESSION,
                    DEFAULT_IN_MEMORY,
                    DEFAULT_BLOCKCACHE,
                    Integer.MAX_VALUE,
                    DEFAULT_BLOOMFILTER));
            desc.addFamily(new HColumnDescriptor(DEFAULT_FAMILY));
            desc.addFamily(new HColumnDescriptor(SNAPSHOT_ISOLATION_CHILDREN_FAMILY));
            return desc;
	}
	
	public static HColumnDescriptor createDataFamily() {
		return new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
				SpliceConstants.DEFAULT_VERSIONS,
				SpliceConstants.DEFAULT_COMPRESSION,
				SpliceConstants.DEFAULT_IN_MEMORY,
				SpliceConstants.DEFAULT_BLOCKCACHE,
				SpliceConstants.DEFAULT_TTL,
				SpliceConstants.DEFAULT_BLOOMFILTER);
	}
	
    public static HColumnDescriptor createTransactionFamily() {
        final HColumnDescriptor siFamily = new HColumnDescriptor(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES);
        siFamily.setMaxVersions(Integer.MAX_VALUE);
        siFamily.setTimeToLive(Integer.MAX_VALUE);
        return siFamily;
    }

    

    
    
    
    public static boolean createSpliceHBaseTables () {
        SpliceLogUtils.info(LOG, "Creating Splice Required HBase Tables");
        HBaseAdmin admin = null;
        
        try{
            admin = getAdmin();
            if(!admin.tableExists(TEMP_TABLE_BYTES)){
                HTableDescriptor td = generateDefaultSIGovernedTable(TEMP_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.TEMP_TABLE+" created");
            }
            if(!admin.tableExists(SpliceConstants.TRANSACTION_TABLE_BYTES)){
                HTableDescriptor td = generateTransactionTable(TRANSACTION_TABLE);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.TRANSACTION_TABLE_BYTES+" created");
            }

            if(!admin.tableExists(SpliceConstants.CONGLOMERATE_TABLE_NAME_BYTES)){
                HTableDescriptor td = generateDefaultSIGovernedTable(CONGLOMERATE_TABLE_NAME);
                admin.createTable(td);
                SpliceLogUtils.info(LOG, SpliceConstants.CONGLOMERATE_TABLE_NAME_BYTES+" created");
            }

            
            return true;
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"Unable to set up HBase Tables",e);
            return false;
        }finally{
        	Closeables.closeQuietly(admin);
        }
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
