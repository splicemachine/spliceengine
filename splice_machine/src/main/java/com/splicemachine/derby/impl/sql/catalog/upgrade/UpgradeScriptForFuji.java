package com.splicemachine.derby.impl.sql.catalog.upgrade;

import java.util.HashSet;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.catalog.SystemProcedureGenerator;
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
        if (LOG.isInfoEnabled()) LOG.info("Creating 1.0.0 system tables");
        sdd.createFujiTables(tc);
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
