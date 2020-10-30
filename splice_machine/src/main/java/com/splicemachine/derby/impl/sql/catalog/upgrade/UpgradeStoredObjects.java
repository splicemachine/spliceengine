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
import com.splicemachine.db.impl.sql.catalog.BaseDataDictionary;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.si.impl.driver.SIDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class UpgradeStoredObjects extends UpgradeScriptBase {
    public UpgradeStoredObjects(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "intentional")
    @Override
    protected void upgradeSystemTables() throws StandardException {
        try {
            BaseDataDictionary.WRITE_NEW_FORMAT = true;
            BaseDataDictionary.READ_NEW_FORMAT = false;
            tc.rewritePropertyConglomerate();
            sdd.upgradeDataDictionarySerializationToV2(tc);
            UpgradeUtils.upgradeConglomerates(tc, HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
            UpgradeUtils.upgradeConglomerates(tc, HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
            BaseDataDictionary.WRITE_NEW_FORMAT = true;
            BaseDataDictionary.READ_NEW_FORMAT = true;
            dropUnusedTable();

        } catch (Exception e) {
            rollback();
            throw StandardException.plainWrapException(e);
        }
        finally {
            deleteSnapshots();
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

    private void rollback() throws StandardException {
        sdd.rollbackDataDictionarySerializationToV2(tc);
        try {
            List<String> tableNames = Arrays.asList("16",
                    HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME,
                    HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
            UpgradeUtils.rollBack(tableNames);
        } catch (IOException ioe) {
            throw StandardException.plainWrapException(ioe);
        }
    }

    private void deleteSnapshots() throws StandardException {
        try {
            sdd.cleanupSerdeUpgrade(tc);
            List<String> snapshotNames = Arrays.asList("16_snapshot",
                    HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME + "_snapshot",
                    HBaseConfiguration.CONGLOMERATE_TABLE_NAME + "_snapshot");
            UpgradeUtils.deleteSnapshots(snapshotNames);
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
