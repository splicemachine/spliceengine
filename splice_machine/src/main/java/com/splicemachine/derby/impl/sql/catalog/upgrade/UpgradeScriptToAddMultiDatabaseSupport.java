/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

import java.util.Properties;

public class UpgradeScriptToAddMultiDatabaseSupport extends UpgradeScriptBase {
    public UpgradeScriptToAddMultiDatabaseSupport(SpliceDataDictionary sdd, TransactionController tc, Properties startParams) {
        super(sdd, tc);
        this.startParams = startParams;
    }

    private Properties startParams;

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.createSysDatabasesTableAndAddDatabaseIdColumnsToSysTables(tc, startParams);

        SpliceLogUtils.info(LOG, "Catalog upgraded to multidb compliance");
    }
}
