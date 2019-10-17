package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Created by yxia on 10/10/19.
 */
public class UpgradeScriptToUpdateViewForSYSCONGLOMERATEINSCHEMAS extends UpgradeScriptBase {
    public UpgradeScriptToUpdateViewForSYSCONGLOMERATEINSCHEMAS(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.updateSystemViewForSysConglomerates(tc);

        SpliceLogUtils.info(LOG, "Catalog upgraded: updated system view sysvw.sysconglomerateinschemas");

    }
}
