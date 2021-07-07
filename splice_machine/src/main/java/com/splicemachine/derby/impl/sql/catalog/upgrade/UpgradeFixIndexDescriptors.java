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
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.BaseDataDictionary;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.sql.catalog.Splice_DD_Version;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

public class UpgradeFixIndexDescriptors extends UpgradeScriptBase {
    public UpgradeFixIndexDescriptors(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        for (TableDescriptor td: sdd.getAllTableDescriptors()) {
            SchemaDescriptor sd = td.getSchemaDescriptor();
            for (ConglomerateDescriptor cd: td.getConglomerateDescriptors()) {
                if (cd.isIndex()) {
                    fixIndexDescriptorColumns(td, sd, cd);
                }
                addKeyFormatIdsToConglomerate(cd);
            }
        }
    }

    private void fixIndexDescriptorColumns(TableDescriptor td, SchemaDescriptor sd, ConglomerateDescriptor cd) throws StandardException {
        IndexDescriptor id = cd.getIndexDescriptor();
        if (id.isInvalidIndexDescriptor()) {
            sdd.dropConglomerateDescriptor(cd, tc);
            /* At this point, baseStorageColumnDescription mistakenly contains the LOGICAL column ids, and
             * baseColumnDescription contains nothing.
             * We want to recreate the index descriptor and baseStorageColumnDescription should contain the STORAGE
             * columns ids, whereas baseColumnDescription should contain the LOGICAL ids
             */
            id.setBaseColumnPositions(id.getBaseColumnStoragePositions());
            id.setBaseColumnStoragePositions(
                    Arrays.stream(id.getBaseColumnPositions())
                            .map(colPos -> td.getColumnDescriptor(colPos).getStoragePosition())
                            .toArray()
            );
            sdd.addDescriptor(cd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc, false);
        }
    }

    private void addKeyFormatIdsToConglomerate(ConglomerateDescriptor cd) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate)
                ((SpliceTransactionManager) tc).findConglomerate(cd.getConglomerateNumber());
        int[] keyFormatIds = ScanOperation.getKeyFormatIds(conglomerate.getColumnOrdering(), conglomerate.getFormat_ids());
        conglomerate.setKeyFormatIds(keyFormatIds);
        ConglomerateUtils.updateConglomerate(conglomerate, (Txn) ((SpliceTransactionManager) tc).getActiveStateTxn());
    }
}
