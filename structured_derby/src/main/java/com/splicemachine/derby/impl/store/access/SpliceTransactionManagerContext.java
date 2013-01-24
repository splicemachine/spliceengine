package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.log4j.Logger;

final class SpliceTransactionManagerContext extends ContextImpl {
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

	SpliceTransactionManager getTransactionManager() {
		return transactionManager;
	}

	void setTransactionManager(SpliceTransactionManager  transactionManager) {
		this.transactionManager = transactionManager;
	}
}