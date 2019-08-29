/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;

/**
 * Created by jyuan on 10/17/14.
 */
public class UpgradeScriptBase implements UpgradeScript {
	protected static final Logger LOG = Logger.getLogger(UpgradeScriptBase.class);

    protected SpliceDataDictionary sdd;
    protected TransactionController tc;

    public UpgradeScriptBase (SpliceDataDictionary sdd, TransactionController tc) {
        this.sdd = sdd;
        this.tc = tc;
    }

    // TODO: This should get refactored back into DefaultSystemProcedureGenerator or another utility class.
    protected void dropSystemProcedureOrFunction(String sName, String pName, char procedureOrFunction) throws StandardException {
        String schemaName = sName.toUpperCase();
        String procedureName = pName.toUpperCase();

        SchemaDescriptor sd = sdd.getSchemaDescriptor(schemaName, tc, true);  // Throws an exception if the schema does not exist.
        UUID schemaId = sd.getUUID();
        AliasDescriptor ad = sdd.getAliasDescriptor(schemaId.toString(), procedureName, procedureOrFunction);
        if (ad != null) {  // Drop the procedure if it already exists.
            if (LOG.isTraceEnabled()) LOG.trace(String.format("Dropping system %s %s.%s", ad.getDescriptorType().toLowerCase(), sName, pName));
            sdd.dropAliasDescriptor(ad, tc);
        }
    }

    protected void upgradeSystemTables() throws StandardException {
    }

    protected void upgradeSystemProcedures() throws StandardException{
        if(LOG.isInfoEnabled()) LOG.info("Upgrading system procedures");
        sdd.createOrUpdateAllSystemProcedures(tc);
    }

    protected void upgradeSystemFunctions() throws StandardException {

    }

    protected void upgradeSystemStoredStatements() throws StandardException {
        if(LOG.isInfoEnabled()) LOG.info("Updating system prepared statements");
        sdd.updateMetadataSPSes(tc);
    }

    public void run() throws StandardException{
    	upgradeSystemTables();
    	upgradeSystemProcedures();
    	upgradeSystemFunctions();
    	upgradeSystemStoredStatements();
    }
}
