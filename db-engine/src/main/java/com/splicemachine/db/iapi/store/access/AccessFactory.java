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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.locks.LockFactory;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.store.access.conglomerate.MethodFactory;

/**

  Module interface for an access manager.  An access manager provides
  transactional access via access methods to data in a single storage
  manager.
  <p>
  An AccessFactory is typically obtained from the Monitor:
  <p>
  <blockquote><pre>
	// Get the current transaction controller.
	AccessFactory af;
	af = (AccessFactory) Monitor.findServiceModule(this, AccessFactory.MODULE);
  </pre></blockquote>
**/

public interface AccessFactory
{
	/**
	 * Used to identify this interface when finding it with the Monitor.
     **/
	String MODULE =
        "com.splicemachine.db.iapi.store.access.AccessFactory";

	/**
	 * Register an access method that this access manager can use.
	 **/
	void registerAccessMethod(MethodFactory factory);

	/**
	 * Database creation has finished.
     *
	 * @exception StandardException Standard exception policy.
	 **/
	void createFinished() throws StandardException;

	/**
	 *Find an access method that implements an implementation type.
     *
	 * @exception StandardException Standard exception policy.
	 **/
	MethodFactory findMethodFactoryByImpl(String impltype)
        throws StandardException;

	/**
	 * Find an access method that implements a format type.
	 **/
	MethodFactory findMethodFactoryByFormat(UUID format);

    /**
     * Get the LockFactory to use with this store.
     *
	 * @return The lock factory to use with this store.
     *
     **/
	LockFactory getLockFactory();


    /**
     * Return the XAResourceManager associated with this AccessFactory.
     * <p>
     * Returns an object which can be used to implement the "offline" 
     * 2 phase commit interaction between the accessfactory and outstanding
     * transaction managers taking care of in-doubt transactions.
     *
     * @return The XAResourceManager associated with this accessfactory.
     *
	 * @exception StandardException Standard exception policy.
     *
     **/
	/* XAResourceManager */ Object getXAResourceManager()
		throws StandardException;


	/**
	 * Is the store read-only.
	 */
	boolean isReadOnly();



    /**************************************************************************
     * methods that are Property related.
     **************************************************************************
     */


    /**************************************************************************
     * methods that are transaction related.
     **************************************************************************
     */

	/**
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Implicitly creates an access context if one
	 * does not already exist.
     *
     * @param cm    The context manager for the current context.
     *
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/

	TransactionController getTransaction(ContextManager cm)
		throws StandardException;

    /**
     * Get a transaction. If a new transaction is 
     * implicitly created, give it name transName.
     *
     * @param cm            The context manager for the current context.
     * @param transName     If a new transaction is started, it will be given 
     *                      this name.  The name is displayed in the 
     *                      transactiontable VTI.
     *
	 * @exception StandardException Standard exception policy.
     *
	 * @see TransactionController
	 * @see AccessFactory#getTransaction
     */
	TransactionController getAndNameTransaction(
    ContextManager  cm, 
    String          transName)
		throws StandardException;

    /**
     * Return a snap shot of all transactions in the db.
     * <p>
     * Take a snap shot of all transactions currently in the database and make
     * a record of their information.
     *
     * @return an array of TransactionInfo, or null if there is 
     *         no transaction in the database.
     *
     **/
	TransactionInfo[] getTransactionInfo();

	/**
     * Start a global transaction.
     * <p>
	 * Get a transaction controller with which to manipulate data within
	 * the access manager.  Implicitly creates an access context.
     * <p>
     * Must only be called if no other transaction context exists in the
     * current context manager.  If another transaction exists in the context
     * an exception will be thrown.
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.
     * <p>
     * If the global transaction id given matches an existing in-doubt global
     * transaction in the current system, then a StandardException will
     * be thrown with a state of SQLState.STORE_XA_XAER_DUPID.
     * <p>
     *
     * @param cm        The context manager for the current context.
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie.
     *                  Xid.getBranchQaulifier()
     *
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/
	/* XATransactionController */ Object startXATransaction(
    ContextManager  cm,
    int             format_id,
    byte[]          global_id,
    byte[]          branch_id)
		throws StandardException;


    /**************************************************************************
     * methods that implement functionality on the 
     *     com.splicemachine.db.iapi.db API
     **************************************************************************
     */

	/**
	  * Freeze the database temporarily so a backup can be taken.
	  * <P>Please see Derby on line documentation on backup and restore.
	  *
	  * @exception StandardException Thrown on error
	  */
	void freeze() throws StandardException;

	/**
	  * Unfreeze the database after a backup has been taken.
	  * <P>Please see Derby on line documentation on backup and restore.
	  *
	  * @exception StandardException Thrown on error
	  */
	void unfreeze() throws StandardException;

	/**
     * Backup the database to backupDir.  
     * <P>Please see Derby on line documentation on backup and restore.
     *
     * @param backupDir the name of the directory where the backup should be
     *		             stored.
     * @param wait      if <tt>true</tt>, waits for  all the backup blocking 
     *                  operations in progress to finish.
     *
     * @exception StandardException Thrown on error
     */
	void backup(String backupDir, boolean wait)
        throws StandardException;


	/**
	 * Checkpoints the database, that is, flushes all dirty data to disk.
	 * Records a checkpoint in the transaction log, if there is a log.
	 *
	 * @exception StandardException Thrown on error
	 */
	void checkpoint() throws StandardException;

	/**
	 * Get a transaction controller with which to query data within
	 * the access manager.  Implicitly creates an access context if one
	 * does not already exist.
	 *
	 * @param cm    The context manager for the current context.
	 *
	 * @exception StandardException Standard exception policy.
	 * @see TransactionController
	 **/
	TransactionController getReadOnlyTransaction(ContextManager cm, long txnId) throws StandardException;
}
