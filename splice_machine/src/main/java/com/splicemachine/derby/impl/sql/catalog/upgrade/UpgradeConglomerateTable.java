package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

public class UpgradeConglomerateTable extends UpgradeScriptBase {

    private static boolean tableCreated = false;
    public UpgradeConglomerateTable(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        SpliceLogUtils.info(LOG, "Creating SPLICE_CONGLOMERATE_SI table...");
        PartitionAdmin admin = null;
        try {
            admin = SIDriver.driver().getTableFactory().getAdmin();
            if (admin.tableExists(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME)){
                admin.deleteTable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
            }
            admin.createSITable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
            admin.setCatalogVersion(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME, "1");
            UpgradeUtils.initializeConglomerateSITable(tc);
            tableCreated = true;
        }
        catch (Exception e) {
            // If upgrade fails for some reasons, roll back CONGLOMERATE_SI table and throw an exception
            rollBack();
            throw StandardException.plainWrapException(e);
        }
    }

    public static boolean isTableCreated() {
        return tableCreated;
    }

    public static void rollBack() throws StandardException {
        try {
            PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
            if (admin.tableExists(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME)) {
                admin.deleteTable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
