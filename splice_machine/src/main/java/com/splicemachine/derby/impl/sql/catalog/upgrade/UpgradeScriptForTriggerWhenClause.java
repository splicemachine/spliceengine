package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

/**
 * Created by msirek on 11/5/19.
 */
public class UpgradeScriptForTriggerWhenClause extends UpgradeScriptBase {
    public UpgradeScriptForTriggerWhenClause(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    @SuppressFBWarnings(value="REC_CATCH_EXCEPTION", justification="Intentional")
    protected void upgradeSystemTables() throws StandardException {
        sdd.upgradeAddColumnToSystemTable(tc, DataDictionary.SYSTRIGGERS_CATALOG_NUM, new int[]{18}, null);
    }
}
