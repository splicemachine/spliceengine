package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

public class UpgradeSnapshotTables extends UpgradeScriptBase {

    public UpgradeSnapshotTables(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }


    @Override
    protected void upgradeSystemTables() throws StandardException {
        LOG.info("Upgrading snapshot system tables.");
        try {
            sdd.upgradeSnapshotSystemTables(tc);
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
        LOG.info("Upgraded snapshot system tables.");
    }
}
