package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

public class UpgradeScriptForTablePriorities extends UpgradeScriptBase {

    public UpgradeScriptForTablePriorities(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        SpliceLogUtils.info(LOG, "Upgrading table priorities...");
        // Exception handling here is WIP obviously
        try {
            int num = sdd.upgradeTablePriorities(tc);
            SpliceLogUtils.info(LOG, "Upgraded " + num + " table priorities.");
        }
        catch( Exception e) {
            SpliceLogUtils.info(LOG, "Error while upgrading: " + e.toString());
            throw Exceptions.parseException(e);
        }
    }
}
