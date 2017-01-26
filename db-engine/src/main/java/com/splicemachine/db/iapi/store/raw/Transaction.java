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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.store.raw;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.services.property.PersistentSet;

/**
*/

public interface Transaction {

	/**
		Return the context manager this transaction is associated with.
	*/
	public ContextManager getContextManager();

    /**
     * Get the compatibility space of the transaction.
     * <p>
     * Returns an object that can be used with the lock manager to provide
     * the compatibility space of a transaction.  2 transactions with the
     * same compatibility space will not conflict in locks.  The usual case
     * is that each transaction has it's own unique compatibility space.
     * <p>
     *
	 * @return The compatibility space of the transaction.
     **/
    CompatibilitySpace getCompatibilitySpace();

    /**
     * Tell this transaction whether it should time out immediately if a lock
     * cannot be granted without waiting. This could be used in a nested
     * transaction to prevent long waits if there is a lock conflict between
     * the nested transaction and its parent. If it is used this way, the
     * calling code should catch timeout exceptions from the nested transaction
     * and retry the operation (without disabling waiting) in the parent
     * transaction.
     *
     * @param noWait if {@code true} never wait for a lock in this transaction,
     * but time out immediately
     * @see com.splicemachine.db.iapi.services.locks.LockOwner#noWait()
     */
    void setNoLockWait(boolean noWait);

	/**
		Called after the transaction has been attached to an Access Manger
		TransactionController. Thus may not be called for all transactions.
		Purpose is to allow a transaction access to database (service) properties.

		Will not be called for transactions early in the boot process, ie. before
		the property conglomerate is set up.
		@exception StandardException  Standard Derby exception policy
	*/
	public void setup(PersistentSet set)
		throws StandardException;

	/**
		Return my transaction identifier. Transaction identifiers may be 
        re-used for transactions that do not modify the raw store.
		May return null if this transaction has no globalId.
	*/
	public GlobalTransactionId getGlobalId();

	/**
		Commit this transaction. All savepoints within this transaction are 
        released.

		@return the commit instant of this transaction, or null if it
		didn't make any changes 
		
		@exception StandardException
        A transaction level exception is thrown
        if the transaction was aborted due to some error. Any exceptions that 
        occur of lower severity than Transaction severity are caught, the 
        transaction is then aborted and then an exception of Transaction
		severity is thrown nesting the original exception.

		@exception StandardException Any exception more severe than a
        Transaction exception is not caught and the transaction is not aborted.
        The transaction will be aborted by the standard context mechanism.

	*/

	public void commit() throws StandardException;

	/**
	    "Commit" this transaction without sync'ing the log.
		Everything else is identical to commit(), use this at your own risk.
		
		<BR>bits in the commitflag can turn on to fine tuned the "commit":
		KEEP_LOCKS - no locks will be released by the commit and no post commit
		processing will be initiated.  If, for some reasons, the locks cannot be
		kept even if this flag is set, then the commit will sync the log, i.e.,
		it will revert to the normal commit.

		@exception StandardException
        A transaction level exception is thrown
        if the transaction was aborted due to some error. Any exceptions that 
        occur of lower severity than Transaction severity are caught, the 
        transaction is then aborted and then an exception of Transaction
		severity is thrown nesting the original exception.

		@exception StandardException Any exception more severe than a
        Transaction exception is not caught and the transaction is not aborted.
        The transaction will be aborted by the standard context mechanism.
	*/

	public void commitNoSync(int commitflag) throws StandardException;
	public final int RELEASE_LOCKS = TransactionController.RELEASE_LOCKS;
	public final int KEEP_LOCKS = TransactionController.KEEP_LOCKS;


	/**
		Abort all changes made by this transaction since the last commit, abort
		or the point the transaction was started, whichever is the most recent.
		All savepoints within this transaction are released.

		@exception StandardException Only exceptions with severities greater 
        than ExceptionSeverity.TRANSACTION_SEVERITY will be thrown.
		
	*/
	public void abort() throws StandardException;

	/**
		Close this transaction, the transaction must be idle. This close will
		pop the transaction context off the stack that was pushed when the 
        transaction was started.

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Derby error policy
		@exception StandardException A transaction level exception is 
        thrown if the transaction is not idle.

		
	*/
	public void close() throws StandardException;

	/**
        If this transaction is not idle, abort it.  After this call close().

		@see RawStoreFactory#startTransaction

		@exception StandardException Standard Derby error policy
		@exception StandardException A transaction level exception is 
        thrown if the transaction is not idle.

		
	*/
	public void destroy() throws StandardException;


	/**
		Set a save point in the current transaction. A save point defines a 
        point in time in the transaction that changes can be rolled back to. 
        Savepoints can be nested and they behave like a stack. Setting save 
        points "one" and "two" and the rolling back "one" will rollback all 
        the changes made since "one" (including those made since "two") and 
        release savepoint "two".
    @param name     The user provided name of the savepoint
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard Derby exception policy
		@exception StandardException
        A statement level exception is thrown if a savepoint already 
        exists in the current transaction with the same name.
		
	*/

	public int setSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
		Release the save point of the given name. Relasing a savepoint removes 
        all knowledge from this transaction of the named savepoint and any 
        savepoints set since the named savepoint was set.
    @param name     The user provided name of the savepoint, set by the user
                    in the setSavePoint() call.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard Derby exception policy
		@exception StandardException
        A statement level exception is thrown if a savepoint already
        exists in the current transaction with the same name.

	*/

	public int releaseSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
		Rollback all changes made since the named savepoint was set. The named
		savepoint is not released, it remains valid within this transaction, and
		thus can be named it future rollbackToSavePoint() calls. Any savepoints
		set since this named savepoint are released (and their changes rolled
        back).
    @param name     The user provided name of the savepoint, set by the user
                    in the setSavePoint() call.
	  @param	kindOfSavepoint	 A NULL value means it is an internal savepoint (ie not a user defined savepoint)
                    Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
                    A String value for kindOfSavepoint would mean it is SQL savepoint
                    A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint

		@return returns total number of savepoints in the stack.
		@exception StandardException  Standard Derby exception policy
		@exception StandardException
        A statement level exception is thrown if no savepoint exists with 
        the given name.

	*/
	public int rollbackToSavePoint(String name, Object kindOfSavepoint) throws StandardException;

	/**
		Add to the list of post commit work that may be processed after this
		transaction commits.  If this transaction aborts, then the post commit
		work list will be thrown away.  No post commit work will be taken out
		on a rollback to save point.

		@param work the post commit work that is added
	*/
	public void addPostCommitWork(Serviceable work);

	/**
		Add to the list of post termination work that may be processed after this
		transaction commits or aborts.

		@param work the post termination work that is added
	*/
	public void addPostTerminationWork(Serviceable work);

    /**
     * Reveals whether the transaction has ever read or written data.
     *
	 * @return true If the transaction has never read or written data.
     **/
	public boolean isIdle();

    /**
	  Reveal whether the transaction is in a pristine state, which
	  means it hasn't done any updates since the last commit.
	  @return true if so, false otherwise
	  */
    public boolean isPristine();

	/**
		Get an object to handle non-transactional files.
	*/
	public FileResource getFileHandler();

	/**
		Return true if any transaction is blocked, even if not by this one.
	 */
	public  boolean anyoneBlocked();

	/**
     * Convert a local transaction to a global transaction.
     * <p>
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Tbis controller allows one to manipulate a 
     * global XA conforming transaction.
     * <p>
     * Must only be called a previous local transaction was created and exists
     * in the context.  Can only be called if the current transaction is in
     * the idle state.  
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.  
     *
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie. 
     *                  Xid.getBranchQaulifier()
     * 	
	 * @exception StandardException Standard exception policy.
	 **/
	void createXATransactionFromLocalTransaction(
    int                     format_id,
    byte[]                  global_id,
    byte[]                  branch_id)
		throws StandardException;

    /**
     * This method is called to commit the current XA global transaction.
     * <p>
     * RESOLVE - how do we map to the "right" XAExceptions.
     * <p>
     *
     *
     * @param onePhase If true, the resource manager should use a one-phase
     *                 commit protocol to commit the work done on behalf of 
     *                 current xid.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_commit(
    boolean onePhase)
		throws StandardException;

    /**
     * This method is called to ask the resource manager to prepare for
     * a transaction commit of the transaction specified in xid.
     * <p>
     *
     * @return         A value indicating the resource manager's vote on the
     *                 the outcome of the transaction.  The possible values
     *                 are:  XA_RDONLY or XA_OK.  If the resource manager wants
     *                 to roll back the transaction, it should do so by 
     *                 throwing an appropriate XAException in the prepare
     *                 method.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int xa_prepare()
		throws StandardException;
    public static final int XA_RDONLY = 1; 
    public static final int XA_OK     = 2; 

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void xa_rollback()
        throws StandardException;

	
    /**
	 * get string ID of the actual transaction ID that will 
	 * be used when transaction is in  active state. 
	 */
	public String getActiveStateTxIdString();


    /**
     * Get DataValueFactory.
     * <p>
     * Return a DataValueFactory that can be used to allocate objects.  Used
     * to make calls to: 
     *     DataValueFactory.getInstanceUsingFormatIdAndCollationType()
     *
	 * @return a booted data value factory.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DataValueFactory getDataValueFactory()
		throws StandardException;
}
