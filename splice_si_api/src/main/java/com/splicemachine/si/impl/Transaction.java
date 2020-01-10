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

package com.splicemachine.si.impl;

import java.io.IOException;

/**
*/

public interface Transaction {


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
    void setNoLockWait(boolean noWait) throws IOException;


	/**
		Commit this transaction. All savepoints within this transaction are 
        released.

		@return the commit instant of this transaction, or null if it
		didn't make any changes 
	*/

	void commit() throws IOException;

	/**
	    "Commit" this transaction without sync'ing the log.
		Everything else is identical to commit(), use this at your own risk.
		
		<BR>bits in the commitflag can turn on to fine tuned the "commit":
		KEEP_LOCKS - no locks will be released by the commit and no post commit
		processing will be initiated.  If, for some reasons, the locks cannot be
		kept even if this flag is set, then the commit will sync the log, i.e.,
		it will revert to the normal commit.
	*/

	void commitNoSync(int commitflag) throws IOException;

	/**
		Abort all changes made by this transaction since the last commit, abort
		or the point the transaction was started, whichever is the most recent.
		All savepoints within this transaction are released.

	*/
	void abort() throws IOException;

	/**
		Close this transaction, the transaction must be idle. This close will
		pop the transaction context off the stack that was pushed when the 
        transaction was started.
	*/
	void close() throws IOException;

	/**
        If this transaction is not idle, abort it.  After this call close().

	*/
	void destroy() throws IOException;


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
        A statement level exception is thrown if a savepoint already
        exists in the current transaction with the same name.
		
	*/

	int setSavePoint(String name, Object kindOfSavepoint) throws IOException;

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

	*/

	int releaseSavePoint(String name, Object kindOfSavepoint) throws IOException;

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

	*/
	int rollbackToSavePoint(String name, Object kindOfSavepoint) throws IOException;

    /**
     * Reveals whether the transaction has ever read or written data.
     *
	 * @return true If the transaction has never read or written data.
     **/
	boolean isIdle();

    /**
	  Reveal whether the transaction is in a pristine state, which
	  means it hasn't done any updates since the last commit.
	  @return true if so, false otherwise
	  */
	boolean isPristine();

	/**
		Return true if any transaction is blocked, even if not by this one.
	 */
	boolean anyoneBlocked();

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
	 **/
	void createXATransactionFromLocalTransaction(
			int format_id,
			byte[] global_id,
			byte[] branch_id)
			throws IOException;

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
     **/
	void xa_commit(
			boolean onePhase)
			throws IOException;

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
     **/
	int xa_prepare()
			throws IOException;

    /**
     * rollback the current global transaction.
     * <p>
     * The given transaction is roll'ed back and it's history is not
     * maintained in the transaction table or long term log.
     * <p>
     *
     **/
	void xa_rollback()
			throws IOException;

	
    /**
	 * get string ID of the actual transaction ID that will 
	 * be used when transaction is in  active state. 
	 */
	String getActiveStateTxIdString();
}
