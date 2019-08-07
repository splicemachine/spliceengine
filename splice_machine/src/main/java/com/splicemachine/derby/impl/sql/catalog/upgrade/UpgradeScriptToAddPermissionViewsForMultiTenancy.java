package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Created by yxia on 7/15/19.
 */
public class UpgradeScriptToAddPermissionViewsForMultiTenancy extends UpgradeScriptBase {
    public UpgradeScriptToAddPermissionViewsForMultiTenancy(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.createPermissionTableSystemViews(tc);

        SpliceLogUtils.info(LOG, "Catalog upgraded: added system views for permission tables");

    }
}
