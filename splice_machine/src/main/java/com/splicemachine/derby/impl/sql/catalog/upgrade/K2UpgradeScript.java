/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.DefaultInfoImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.pipeline.ErrorState;
import org.apache.log4j.Logger;

import java.sql.Types;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class K2UpgradeScript extends UpgradeScriptBase {
    private static final Logger LOG = Logger.getLogger(K2UpgradeScript.class);
    public K2UpgradeScript(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        super.upgradeSystemTables();

        sdd.getSystemAggregateGenerator().createAggregates(tc);
        sdd.dropStatisticsTables(tc);
        sdd.createStatisticsTables(tc);
        sdd.createSchemasPermsTables(tc);

        // drop STATS_TOP_K procedure
        AliasDescriptor procedure = sdd.getAliasDescriptor(sdd.getSysFunSchemaDescriptor().getUUID().toString(), "STATS_TOP_K", AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);
        if (procedure != null) {
            sdd.dropAliasDescriptor(procedure, tc);
        } else {
            LOG.warn("We couldn't remove the 'STATS_TOP_K' procedure");
        }
    }
}
