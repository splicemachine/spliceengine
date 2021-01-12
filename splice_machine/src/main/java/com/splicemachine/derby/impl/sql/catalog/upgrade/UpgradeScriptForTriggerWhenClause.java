package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.SYSTRIGGERSRowFactory;
import com.splicemachine.db.impl.sql.catalog.TabInfoImpl;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseController;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Created by msirek on 11/5/19.
 */
public class UpgradeScriptForTriggerWhenClause extends UpgradeScriptBase {
    public UpgradeScriptForTriggerWhenClause(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.upgradeAddColumnToSystemTable(tc, DataDictionary.SYSTRIGGERS_CATALOG_NUM, new int[]{18});
    }
}
