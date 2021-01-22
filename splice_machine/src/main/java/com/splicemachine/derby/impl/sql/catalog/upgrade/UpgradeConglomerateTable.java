package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;

public class UpgradeConglomerateTable extends UpgradeScriptBase {
    public UpgradeConglomerateTable(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        SpliceLogUtils.info(LOG, "Creating SPLICE_CONGLOMERATE_SI table...");
        PartitionAdmin admin = null;
        try {
            admin = SIDriver.driver().getTableFactory().getAdmin();
            if (!admin.tableExists(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME)) {
                admin.createSITable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
                UpgradeUtils.initializeConglomerateSITable(tc);
            }
        }
        catch (IOException e) {
            // If upgrade fails for some reasons, roll back CONGLOMERATE_SI table and throw an exception
            try {
                if (admin.tableExists(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME)) {
                    admin.deleteTable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
                }
            }
            catch (IOException ioe) {
                throw StandardException.plainWrapException(ioe);
            }
            throw StandardException.plainWrapException(e);
        }
    }
}
