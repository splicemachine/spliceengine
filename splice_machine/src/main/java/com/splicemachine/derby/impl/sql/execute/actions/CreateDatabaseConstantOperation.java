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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.DDLConstantAction;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

public class CreateDatabaseConstantOperation extends DDLConstantAction {
    private static final Logger LOG = Logger.getLogger(CreateDatabaseConstantOperation.class);
    private final String dbName;
    /**
     * Make the ConstantAction for a CREATE DATABASE statement.
     *
     *  @param dbName    Name of table.
     */
    public CreateDatabaseConstantOperation(String dbName) {
        SpliceLogUtils.trace(LOG, "CreateDatabaseConstantOperation {%s}",dbName);
        this.dbName = dbName;
    }

    public String toString() {
        return "CREATE DATABASE " + dbName;
    }

    /**
     *    This is the guts of the Execution-time logic for CREATE DATABASE.
     *
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction(Activation)
     *
     * @exception StandardException        Thrown on failure
     */
    @Override
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");
        executeConstantActionMinion(activation,
                activation.getLanguageConnectionContext().getTransactionExecute());
    }

    /**
     *    This is the guts of the Execution-time logic for CREATE DATABASE.
     *  This is variant is used when we to pass in a tc other than the default
     *  used in executeConstantAction(Activation).
     *
     * @param activation current activation
     * @param tc transaction controller
     *
     * @exception StandardException        Thrown on failure
     */
    public void executeConstantAction(Activation activation,TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantAction");
        executeConstantActionMinion(activation, tc);
    }

    private boolean isDatabasePresent(Activation activation) throws StandardException {
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        return dd.getDatabaseDescriptor(dbName, lcc.getTransactionExecute(), false) != null;
    }

    private void executeConstantActionMinion(Activation activation,TransactionController tc) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActionMinion");

        if (isDatabasePresent(activation)) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS, "Database" , dbName);
        }

        /*
         * Inform the data dictionary that we are about to write to it.
         * There are several calls to data dictionary "get" methods here
         * that might be done in "read" mode in the data dictionary, but
         * it seemed safer to do this whole operation in "write" mode.
         *
         * We tell the data dictionary we're done writing at the end of
         * the transaction.
         */
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        lcc.getDataDictionary().startWriting(lcc);
        lcc.getDataDictionary().createNewDatabase(dbName);

        if (!isDatabasePresent(activation)) {
            throw StandardException.newException(SQLState.CREATE_DATABASE_FAILED, dbName);
        }

        // XXX (arnaud multidb) create owner of DB with some syntax here?
    }

    public String getScopeName() {
        return String.format("Create Database %s", dbName);
    }
}
