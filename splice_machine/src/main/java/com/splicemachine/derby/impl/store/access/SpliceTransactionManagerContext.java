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

		if (!abortAll && (error instanceof StandardException))
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
