package com.splicemachine.derby.impl.sql.catalog.upgrade;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

/**
 * Created by jyuan on 10/17/14.
 */
public abstract class UpgradeScriptBase implements UpgradeScript {
	protected static final Logger LOG = Logger.getLogger(UpgradeScriptBase.class);

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
    	if (LOG.isInfoEnabled()) LOG.info("Upgrading system tables");
        upgradeSystemTables();
    	if (LOG.isInfoEnabled()) LOG.info("Upgrading system procedures");
        upgradeSystemProcedures();
    	if (LOG.isInfoEnabled()) LOG.info("Upgrading system functions");
        upgradeSystemFunctions();
    	if (LOG.isInfoEnabled()) LOG.info("Upgrading system stored statements");
        upgradeSystemStoredStatements();
    }
}
