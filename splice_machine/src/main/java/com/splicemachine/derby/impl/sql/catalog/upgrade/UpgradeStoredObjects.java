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

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;

public class UpgradeStoredObjects extends UpgradeScriptBase {
    public UpgradeStoredObjects(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        try {
            dropUnusedTable();
            UpgradeUtils.upgradeConglomerates(tc, HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private void dropUnusedTable() throws StandardException{
        try {
            PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
            if (admin.tableExists(HBaseConfiguration.TENTATIVE_TABLE)) {
                admin.deleteTable(HBaseConfiguration.TENTATIVE_TABLE);
            }
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
