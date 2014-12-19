package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.sql.SQLException;

import com.splicemachine.derby.impl.temp.TempTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.log4j.Logger;

/**
 * Utility to split the TEMP table.
 */
public class TempSplit {
	
	//
	// IMPORTANT: please be aware this does much more than 'split' the temp table.
	// In fact, it doesn't do that at all. Rather, it disables and removes
	// the SPLICE_TEMP table and creates a brand new one with preset region count.
	// and split keys. Only use this if you know what you are doing.
	// 
	
	private static final Logger LOG = Logger.getLogger(TempSplit.class);
 
	/**
	 * Split the TEMP table into a fixed number of buckets.
	 *
	 * It disables and removes the TEMP table and creates a new one
	 * with the right split keys.
	 */
	public static void SYSCS_SPLIT_TEMP() throws SQLException{
 
		SpliceLogUtils.info(LOG, "Recreating SPLICE_TEMP table from scratch and splitting into preset buckets...");
		
        HBaseAdmin admin = SpliceUtilities.getAdmin();
        try {
    		SpliceLogUtils.info(LOG, "Recreating SPLICE_TEMP table (step 1 - disable)...");
            admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to disable the TEMP table",e);
        }

        try {
    		SpliceLogUtils.info(LOG, "Recreating SPLICE_TEMP table (step 2 - delete)...");
            admin.deleteTable(SpliceConstants.TEMP_TABLE_BYTES);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to delete the TEMP table",e);
        }
        
        try {
    		SpliceLogUtils.info(LOG, "Recreating SPLICE_TEMP table (step 3 - create)...");
            SpliceUtilities.createTempTable(admin);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to create the TEMP table",e);
            return;
        }
        
        try {
    		SpliceLogUtils.info(LOG, "Recreating SPLICE_TEMP table (step 4 - balance)...");
            boolean runs = admin.balancer();
            if (!runs) {
                SpliceLogUtils.warn(LOG,"Unable to launch the balancer");
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG,"Unable to launch the balancer",e);
        }

		SpliceLogUtils.info(LOG, "Done cecreating SPLICE_TEMP table!");
    	
     }
}
