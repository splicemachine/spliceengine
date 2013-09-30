package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;

/**
 * Utility to split the TEMP table.
 */
public class TempSplit{
    private static final Logger LOG = Logger.getLogger(TempSplit.class);

    /**
     * Split the TEMP table into a fixed number of buckets.
     *
     * It disables and removes the TEMP table and creates a new one
     * with the right split keys.
     *
     * @throws SQLException if something goes wrong.
     */
    public static void SYSCS_SPLIT_TEMP() throws SQLException{

        HBaseAdmin admin = SpliceUtilities.getAdmin();
        try {
            admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to disable the TEMP table",e);
        }

        try {
            admin.deleteTable(SpliceConstants.TEMP_TABLE_BYTES);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to delete the TEMP table",e);
        }
        
        try {
            SpliceUtilities.createTempTable(admin);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Unable to create the TEMP table",e);
            return;
        }
        
        try {
            boolean runs = admin.balancer();
            if (!runs) {
                SpliceLogUtils.warn(LOG,"Unable to launch the balancer");
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG,"Unable to launch the balancer",e);
        }

    }
}
