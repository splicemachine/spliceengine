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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.raw.Transaction;
import org.apache.log4j.Logger;

/* A special type of SpliceTransactionManager only used to track the hierarchy
 * of internal transactions, and should not be used from committing transactions
 * or setting or releasing savepoints.
 */
public class SpliceInternalTransactionManager extends SpliceTransactionManager {
    private static Logger LOG = Logger
            .getLogger(SpliceInternalTransactionManager.class);

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    public SpliceInternalTransactionManager(SpliceAccessManager myaccessmanager, Transaction theRawTran,
                                            SpliceTransactionManager parent_transaction, ContextManager cm) {
        super(myaccessmanager, theRawTran, parent_transaction, cm);
    }

    /**************************************************************************
     * Override the methods we want to disallow.
     * It is a coding error to call any of these
     * methods in a SpliceInternalTransactionManager object.
     **************************************************************************
     */

    @Override
    public void commit() throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitNoSync(int commitflag)
            throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int rollbackToSavePoint(String name, boolean close_controllers, Object kindOfSavepoint) throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void xa_commit(boolean onePhase) throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int xa_prepare() throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void xa_rollback() throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPostCommitWork(Serviceable work) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareDataDictionaryChange(String currentDDLChangeId) throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitDataDictionaryChange() throws StandardException {
        throw new UnsupportedOperationException();
    }

}
