package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Created by yxia on 5/5/19.
 */
public class UpgradeScriptForMetadataRestrictionCheck extends UpgradeScriptBase {
    public UpgradeScriptForMetadataRestrictionCheck(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.updateSystemSchemasView(tc);

        SpliceLogUtils.info(LOG, "Catalog upgraded: added support for multi-tenancy");

    }
}
