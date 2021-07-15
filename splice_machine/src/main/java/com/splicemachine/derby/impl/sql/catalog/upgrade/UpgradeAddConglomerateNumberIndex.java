package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.SYSCONGLOMERATESRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSSCHEMASRowFactory;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

public class UpgradeAddConglomerateNumberIndex extends UpgradeScriptBase {

    public UpgradeAddConglomerateNumberIndex(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSCONGLOMERATES_CATALOG_NUM,
                new int[]{SYSCONGLOMERATESRowFactory.SYSCONGLOMERATES_INDEX4_ID});
    }
}
