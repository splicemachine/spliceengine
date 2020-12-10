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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DatabaseDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;

/**
 *    This class  describes actions that are ALWAYS performed for a
 *    DROP DATABASE Statement at Execution time.
 *
 */

public class DropDatabaseConstantOperation extends DDLConstantOperation {
    private final String dbName;
    private final int dropBehavior;
    /**
     *    Make the ConstantAction for a DROP DATABASE statement.
     *
     * @param dbName database name.
     * @param dropBehavior
     *
     */
    public DropDatabaseConstantOperation(String dbName, int dropBehavior) {
        this.dbName = dbName;
        this.dropBehavior = dropBehavior;
    }

    public String toString() {
        return "DROP DATABASE " + dbName + (dropBehavior == StatementType.DROP_CASCADE? " CASCADE": " RESTRICT");
    }

    /**
     *    This is the guts of the Execution-time logic for DROP TABLE.
     *
     *    @see ConstantAction#executeConstantAction
     *
     * @exception StandardException        Thrown on failure
     */
    public void executeConstantAction( Activation activation ) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();

        /*
         * Inform the data dictionary that we are about to write to it.
         * There are several calls to data dictionary "get" methods here
         * that might be done in "read" mode in the data dictionary, but
         * it seemed safer to do this whole operation in "write" mode.
         *
         * We tell the data dictionary we're done writing at the end of
         * the transaction.
         */
        dd.startWriting(lcc);
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        DatabaseDescriptor dbDesc = dd.getDatabaseDescriptor(dbName, tc, true);


        if (dropBehavior == StatementType.DROP_RESTRICT && !dd.isDatabaseEmpty(dbDesc)) {
            throw StandardException.newException(SQLState.LANG_DATABASE_NOT_EMPTY, dbName);
        }
        if (dropBehavior == StatementType.DROP_CASCADE) {
            dropAllDatabaseObjects(activation, dbDesc);
        }

        //dd.dropAllDatabasePermDescriptors(dbDesc.getObjectID(),tc); XXX (arnaud multidb) implement this
        dbDesc.drop(lcc, activation);
        DDLMessage.DDLChange ddlChange = ProtoUtil.createDropDatabase(tc.getActiveStateTxn().getTxnId(), dbName, (BasicUUID)dbDesc.getUUID());
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
    }

    void dropAllDatabaseObjects(Activation activation, DatabaseDescriptor dbDesc) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();

        for (SchemaDescriptor schema : dd.getSchemasInDatabase(dbDesc)) {
            new DropSchemaConstantOperation(
                    dbDesc.getUUID(), schema.getSchemaName(), StatementType.DROP_CASCADE).executeConstantAction(activation);
        }
    }

    public String getScopeName() {
        return String.format("Drop Database %s", dbName);
    }
}
