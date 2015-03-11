/*

   Derby - Class com.splicemachine.db.impl.store.raw.xact.InternalXact

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

package com.splicemachine.db.impl.store.raw.xact;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.store.raw.Transaction;

import com.splicemachine.db.iapi.store.raw.log.LogFactory;

import com.splicemachine.db.iapi.store.raw.data.DataFactory;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.types.DataValueFactory;

/**

	@see Xact

*/
public class InternalXact extends Xact  
{

	/*
	** Constructor
	*/

	protected InternalXact(
    XactFactory         xactFactory, 
    LogFactory          logFactory, 
    DataFactory         dataFactory,
    DataValueFactory    dataValueFactory) 
    {
		super(
            xactFactory, logFactory, dataFactory, dataValueFactory, 
            false, null, false);

		// always want to hold latches & containers open past the commit/abort
		setPostComplete();
	}

	/*
	** Methods of Transaction
	*/

  
	/**
		Savepoints are not supported in internal transactions.

	    @exception StandardException  A transaction exception is thrown to 
                                      disallow savepoints.

		@see Transaction#setSavePoint
	*/
	public int setSavePoint(String name, Object kindOfSavepoint) 
        throws StandardException 
    {
		throw StandardException.newException(
                SQLState.XACT_NOT_SUPPORTED_IN_INTERNAL_XACT);
	}


	/*
	** Methods of RawTransaction
	*/
	/**
		Internal transactions don't allow logical operations.

		@exception StandardException A transaction exception is thrown to 
                                     disallow logical operations.

		@see com.splicemachine.db.iapi.store.raw.xact.RawTransaction#recoveryRollbackFirst
	*/
	
	 public void checkLogicalOperationOk() 
         throws StandardException 
     {
		throw StandardException.newException(
                SQLState.XACT_NOT_SUPPORTED_IN_INTERNAL_XACT);
	 }

	/**
		Yes, we do want to be rolled back first in recovery.

		@see com.splicemachine.db.iapi.store.raw.xact.RawTransaction#recoveryRollbackFirst
	*/
	public boolean recoveryRollbackFirst()
    {
		return true;
	}

	/*
	**	Implementation specific methods
	*/

	/**
	 * @param commitOrAbort to commit or abort
	 *
	 * @exception StandardException on error
	 */
	protected void doComplete(Integer commitOrAbort) 
        throws StandardException 
    {

		// release our latches on an abort
		// keep everything on a commit
		if (commitOrAbort.equals(ABORT))
			super.doComplete(commitOrAbort);
	}

	protected void setIdleState() 
    {

		super.setIdleState();

		// Quiesce mode never denies an internal transaction from going active, don't
		// have to worry about that
		if (countObservers() != 0)
		{
			try
			{
				super.setActiveState();
			}
			catch (StandardException se)
			{
				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT("unexpected exception", se);
			}
		}
	}
}
