/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.store.access.conglomerate;

import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.error.StandardException;


/**

The TransactionManager interface provides methods on the transaction needed
by an access method implementer, but should not be visible to clients of a
TransactionController.
<p>
@see TransactionController

**/

public interface TransactionManager extends TransactionController
{

    /**
     * Constant used for the lock_level argument to openConglomerate() and 
     * openScan() calls.  Pass in MODE_NONE if you want no table or row locks.
     * This is currently only supported from within access.
     **/
    int MODE_NONE      = 5;

    /**
     * release lock immediately after getting lock.
     **/
    int LOCK_INSTANT_DURATION   = 1;
    /**
     * hold lock until end of transaction.
     **/
    int LOCK_COMMIT_DURATION    = 2;
    /**
     * Allow lock to be released manually prior to end transaction.
     **/
    int LOCK_MANUAL_DURATION    = 3;

    /**
     * Add to the list of post commit work.
     * <p>
     * Add to the list of post commit work that may be processed after this
     * transaction commits.  If this transaction aborts, then the post commit
     * work list will be thrown away.  No post commit work will be taken out
     * on a rollback to save point.
     * <p>
     * This routine simply delegates the work to the Rawstore transaction.
     *
     * @param work  The post commit work to do.
     *
     **/
    void addPostCommitWork(Serviceable work);

    /**
     *  Check to see if a database has been upgraded to the required
     *  level in order to use a store feature.
     *
     * @param requiredMajorVersion  required database Engine major version
     * @param requiredMinorVersion  required database Engine minor version
     * @param feature               Non-null to throw an exception, null to 
     *                              return the state of the version match.
     *
     * @return <code> true </code> if the database has been upgraded to 
     *         the required level, <code> false </code> otherwise.
     *
     * @exception  StandardException 
     *             if the database is not at the require version 
     *             when <code>feature</code> feature is 
     *             not <code> null </code>. 
     */
    boolean checkVersion(
            int requiredMajorVersion,
            int requiredMinorVersion,
            String feature)
        throws StandardException;

    /**
     * The ScanManager.close() method has been called on "scan".
     * <p>
     * Take whatever cleanup action is appropriate to a closed scan.  It is
     * likely this routine will remove references to the scan object that it
     * was maintaining for cleanup purposes.
     *
     **/
    void closeMe(ScanManager scan);

    /**
     * The ConglomerateController.close() method has been called on 
     * "conglom_control".
     * <p>
     * Take whatever cleanup action is appropriate to a closed 
     * conglomerateController.  It is likely this routine will remove
     * references to the ConglomerateController object that it was maintaining
     * for cleanup purposes.
     **/
    void closeMe(ConglomerateController conglom_control);

    /**
     * Get an Internal transaction.
     * <p>
     * Start an internal transaction.  An internal transaction is a completely
     * separate transaction from the current user transaction.  All work done
     * in the internal transaction must be physical (ie. it can be undone 
     * physically by the rawstore at the page level, rather than logically 
     * undone like btree insert/delete operations).  The rawstore guarantee's
     * that in the case of a system failure all open Internal transactions are
     * first undone in reverse order, and then other transactions are undone
     * in reverse order.
     * <p>
     * Internal transactions are meant to implement operations which, if 
     * interupted before completion will cause logical operations like tree
     * searches to fail.  This special undo order insures that the state of
     * the tree is restored to a consistent state before any logical undo 
     * operation which may need to search the tree is performed.
     * <p>
     *
	 * @return The new internal transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    TransactionManager getInternalTransaction()
        throws StandardException;

    /**
     * Get the Transaction from the Transaction manager.
     * <p>
     * Access methods often need direct access to the "Transaction" - ie. the
     * raw store transaction, so give access to it.
     *
	 * @return The raw store transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    Transaction getRawStoreXact()
        throws StandardException;
}
