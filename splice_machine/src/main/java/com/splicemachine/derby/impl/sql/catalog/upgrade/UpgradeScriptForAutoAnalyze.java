package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

public class UpgradeScriptForAutoAnalyze extends UpgradeScriptBase {
    public UpgradeScriptForAutoAnalyze(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.upgradeAddColumnToSystemTable(tc, DataDictionary.SYSTABLES_CATALOG_NUM, new int[]{SYSTABLESRowFactory.SYSTABLES_AUTO_ANALYZE});
    }
}
