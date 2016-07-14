/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.shared.common.error.ExceptionSeverity;
import org.apache.log4j.Logger;

public final class SpliceTransactionManagerContext extends ContextImpl {
	private static Logger LOG = Logger.getLogger(SpliceTransactionManagerContext.class);
	private SpliceTransactionManager transactionManager;
	private boolean abortAll;

	public void cleanupOnError(Throwable error) throws StandardException {

		boolean destroy = false;

		if (abortAll == false && (error instanceof StandardException))
		{
			StandardException se = (StandardException) error;

			// If the severity is lower than a transaction error then do nothing.
			if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
				return;

			// If the session is going to disappear then we want to destroy this
			// transaction, not just abort it.
			if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
				destroy = true;
		}
		else
		{
			// abortAll is true or some java* error, throw away the
			// transaction. 
			destroy = true;
		}

		if (transactionManager != null)
		{
			try
			{
				transactionManager.invalidateConglomerateCache();
			}
			catch (StandardException se)
			{
				// RESOLVE - what to do in error case.
				LOG.error("got error while invalidating cache.", se);
			}

			transactionManager.closeControllers(true /* close held controllers */ );
		}

		if (destroy)
		{
			transactionManager = null;
			popMe();
		}
	}

	// this constructor is called with the transaction
	// controller to be saved when the context
	// is created (when the first statement comes in, likely).
	SpliceTransactionManagerContext(ContextManager cm,String context_id,SpliceTransactionManager transactionManager,boolean abortAll) 
			throws StandardException {
		super(cm, context_id);
		this.abortAll = abortAll;
		this.transactionManager = transactionManager;
		transactionManager.setContext(this);
	}

	public SpliceTransactionManager getTransactionManager() {
		return transactionManager;
	}

	void setTransactionManager(SpliceTransactionManager  transactionManager) {
		this.transactionManager = transactionManager;
	}
}