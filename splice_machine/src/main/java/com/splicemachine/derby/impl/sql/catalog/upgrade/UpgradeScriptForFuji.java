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
 */

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import org.apache.log4j.Logger;

/**
 * Created by jyuan on 10/17/14.
 */
public class UpgradeScriptForFuji extends UpgradeScriptBase {
	protected static final Logger LOG = Logger.getLogger(UpgradeScriptForFuji.class);

    public UpgradeScriptForFuji (SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
    }

    @Override
    protected void upgradeSystemProcedures() throws StandardException{

        if (LOG.isInfoEnabled()) LOG.info("Dropping Xplain system procedures");
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_SET_XPLAIN_SCHEMA", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_SET_XPLAIN_MODE", AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
    }

    @Override
    protected void upgradeSystemFunctions() throws StandardException {
    	if (LOG.isInfoEnabled()) LOG.info("Dropping Xplain system functions");
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_GET_XPLAIN_MODE", AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_GET_XPLAIN_SCHEMA", AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);
        // Drop this function because it is has been renamed to SYSCS_GET_RUNTIME_STATISTICS and moved to the SpliceAdmin class.
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_GET_RUNTIMESTATISTICS", AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);
        /*
         * TODO: Rename SYSCS_SET_RUNTIMESTATISTICS to SYSCS_SET_RUNTIME_STATISTICS to match SYSCS_GET_RUNTIME_STATISTICS.
         *       Filed as DB-2321.
         */
    }
}
