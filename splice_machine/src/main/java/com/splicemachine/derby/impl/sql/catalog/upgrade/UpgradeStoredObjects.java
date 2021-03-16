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
import com.splicemachine.derby.impl.sql.catalog.Splice_DD_Version;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

public class UpgradeStoredObjects extends UpgradeScriptBase {
    public UpgradeStoredObjects(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @SuppressFBWarnings(value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification = "intentional")
    @Override
    protected void upgradeSystemTables() throws StandardException {
        try {
            Splice_DD_Version catalogVersion=(Splice_DD_Version)tc.getProperty(SpliceDataDictionary.SPLICE_DATA_DICTIONARY_VERSION);
            String s[] = catalogVersion.toString().split("\\.");
            if (s.length > 3) {
                int sprintNumber = Integer.parseInt(s[3]);
                int minorVersionNumber = Integer.parseInt(s[1]);
                if (minorVersionNumber == 1 && sprintNumber >= BaseDataDictionary.SERDE_UPGRADE_SPRINT) {
                    // if serialization has already changed on 3.1 branch, skip this upgrade
                    SpliceLogUtils.info(LOG, "Skipping data dictionary serialization upgrade for %s",
                            catalogVersion.toString());
                    return;
                }
            }
            SpliceLogUtils.info(LOG, "Start upgrading data dictionary serialization format.");
            // Make a copy of system tables that are to be upgrade. The original table will be truncated. We will
            // read from the copy in old serde format and write to original table in new format. Indexes will be
            // truncated and rebuilt
            UpgradeUtils.cloneConglomerate(sdd, tc);
            BaseDataDictionary.WRITE_NEW_FORMAT = true;
            BaseDataDictionary.READ_NEW_FORMAT = false;

            // rewrite system tables
            sdd.upgradeDataDictionarySerializationToV2(tc);
            // rewrite SPLICE_CONGLOMERATE and SPLICE_CONGLOMERATE_SI
            UpgradeUtils.upgradeConglomerates(tc, HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME);
            UpgradeUtils.upgradeConglomerates(tc, HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
            // rewrite splice:16
            tc.rewritePropertyConglomerate();
            BaseDataDictionary.WRITE_NEW_FORMAT = true;
            BaseDataDictionary.READ_NEW_FORMAT = true;
            dropUnusedTable();
            SpliceLogUtils.info(LOG, "Finished upgrading data dictionary serialization format successfully.");
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, "Failed to upgrade data dictionary serialization format.");
            // If anything is wrong, roll back all dictionary changes. This will clone all snapshots to original tables.
            UpgradeUtils.rollback(sdd, tc);
            throw StandardException.plainWrapException(e);
        }
        finally {
            // Finally delete snapshot and cloned conglomerate.
            UpgradeUtils.deleteSnapshots();
            UpgradeUtils.dropClonedConglomerate(sdd, tc);
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
