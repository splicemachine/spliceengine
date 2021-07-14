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
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.*;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

import java.util.Properties;

public class UpgradeScriptToPrioritizeSchemaIdInSystemIndices extends UpgradeScriptBase {
    public UpgradeScriptToPrioritizeSchemaIdInSystemIndices(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSTABLES_CATALOG_NUM,
                new int[]{SYSTABLESRowFactory.SYSTABLES_INDEX1_ID}
        );

        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSTRIGGERS_CATALOG_NUM,
                new int[]{SYSTRIGGERSRowFactory.SYSTRIGGERS_INDEX2_ID}
        );

        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSSTATEMENTS_CATALOG_NUM,
                new int[]{SYSSTATEMENTSRowFactory.SYSSTATEMENTS_INDEX2_ID}
        );

        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSCONSTRAINTS_CATALOG_NUM,
                new int[]{SYSCONSTRAINTSRowFactory.SYSCONSTRAINTS_INDEX2_ID}
        );

        sdd.upgradeRecreateIndexesOfSystemTable(
                tc,
                DataDictionary.SYSFILES_CATALOG_NUM,
                new int[]{SYSFILESRowFactory.SYSFILES_INDEX1_ID}
        );
    }
}
