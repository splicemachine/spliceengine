/*

   Derby - Class org.apache.derby.impl.sql.execute.LockTableConstantAction

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.sql.Activation;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;


/**
 *	This class describes actions that are ALWAYS performed for a
 *	LOCK TABLE Statement at Execution time.
 *
 */

class LockTableConstantAction implements ConstantAction
{

	private final String					fullTableName;
	private final long					conglomerateNumber;
	private final boolean					exclusiveMode;
	
	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a LOCK TABLE statement.
	 *
	 *  @param fullTableName		Full name of the table.
	 *  @param conglomerateNumber	Conglomerate number for the heap
	 *  @param exclusiveMode		Whether or not to get an exclusive lock.
	 */
	LockTableConstantAction(String fullTableName,
									long conglomerateNumber, boolean exclusiveMode)
	{
		this.fullTableName = fullTableName;
		this.conglomerateNumber = conglomerateNumber;
		this.exclusiveMode = exclusiveMode;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "LOCK TABLE " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for LOCK TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConglomerateController	cc;
		TransactionController	tc;

		/* Get a ConglomerateController for the base conglomerate */
		tc = activation.getTransactionController();

		try
		{
			cc = tc.openConglomerate(
	                conglomerateNumber,
                    false,
					(exclusiveMode) ?
						(TransactionController.OPENMODE_FORUPDATE | 
							TransactionController.OPENMODE_FOR_LOCK_ONLY) :
						TransactionController.OPENMODE_FOR_LOCK_ONLY,
			        TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);
			cc.close();
		}
		catch (StandardException se)
		{
			String msgId = se.getMessageId();
            if (se.isLockTimeoutOrDeadlock())
            {
				String mode = (exclusiveMode) ? "EXCLUSIVE" : "SHARE";
				se = StandardException.newException(
                        SQLState.LANG_CANT_LOCK_TABLE, se, fullTableName, mode);
			}

			throw se;
		}
	}
}
