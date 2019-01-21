package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

/**
 * Created by jyuan on 1/21/19.
 */
public class UpgradeScriptToRemoveUnusedBackupTables extends UpgradeScriptBase {
    public UpgradeScriptToRemoveUnusedBackupTables(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.removeUnusedBackupTables(tc);
        sdd.removeUnusedBackupProcedures(tc);
    }
}
