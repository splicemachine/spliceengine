package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;

/**
 * Created by jyuan on 10/17/14.
 */
public abstract class UpgradeScriptBase implements UpgradeScript {

    protected SpliceDataDictionary sdd;
    protected TransactionController tc;

    public UpgradeScriptBase (SpliceDataDictionary sdd, TransactionController tc) {
        this.sdd = sdd;
        this.tc = tc;
    }

    protected void dropSystemProcedureOrFunction(String sName, String pName, char procedureOrFunction) throws StandardException {
        String schemaName = sName.toUpperCase();
        String procedureName = pName.toUpperCase();

        SchemaDescriptor sd = sdd.getSchemaDescriptor(schemaName, tc, true);  // Throws an exception if the schema does not exist.
        UUID schemaId = sd.getUUID();
        AliasDescriptor ad = sdd.getAliasDescriptor(schemaId.toString(), procedureName, procedureOrFunction);
        if (ad != null) {  // Drop the procedure if it already exists.
            sdd.dropAliasDescriptor(ad, tc);
        }
    }

    protected void upgradeSystemTables() throws StandardException {
    }

    protected void upgradeSystemProcedures() throws StandardException{
    }

    protected void upgradeSystemFunctions() throws StandardException {

    }

    protected void upgradeSystemStoredStatements() {

    }

    public void run() throws StandardException{
        upgradeSystemTables();
        upgradeSystemProcedures();
        upgradeSystemFunctions();
        upgradeSystemStoredStatements();
    }
}