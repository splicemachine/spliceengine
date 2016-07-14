/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
