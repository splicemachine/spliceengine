package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Created by jyuan on 10/17/14.
 */
public class UpgradeScriptForFuji extends UpgradeScriptBase {

    public UpgradeScriptForFuji (SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        sdd.createFujiTables(tc);
    }

    @Override
    protected void upgradeSystemProcedures() throws StandardException{
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_SET_XPLAIN_SCHEMA",
                AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);

        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_SET_XPLAIN_MODE",
                AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR);
    }

    @Override
    protected void upgradeSystemFunctions() throws StandardException {
        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_GET_XPLAIN_MODE",
                AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);

        dropSystemProcedureOrFunction("SYSCS_UTIL", "SYSCS_GET_XPLAIN_SCHEMA",
                AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR);
    }
}
