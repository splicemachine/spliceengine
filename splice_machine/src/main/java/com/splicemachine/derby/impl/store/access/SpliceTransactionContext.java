/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.error.ExceptionSeverity;
import org.apache.log4j.Logger;

public class SpliceTransactionContext extends ContextImpl {
	private static Logger LOG = Logger.getLogger(SpliceTransactionContext.class);
	private SpliceTransaction transaction;
	private     HBaseStore factory;
	private		boolean   abortAll; // true if any exception causes this transaction to be aborted.

	SpliceTransactionContext(ContextManager cm, String name, SpliceTransaction transaction, boolean abortAll, HBaseStore factory) {
		super(cm, name);

		this.transaction = transaction;
		this.abortAll = abortAll;
		this.factory = factory;
		transaction.transContext = this;	// double link between transaction and myself
	}


	/*
	** Context methods (most are implemented by super-class)
	*/


	/**
		@exception StandardException Standard Derby error policy
	*/
	public void cleanupOnError(Throwable error) throws StandardException {
		boolean throwAway = false;

		if (error instanceof StandardException) {
			StandardException se = (StandardException) error;

			if (abortAll) {
				// any error aborts an internal/nested xact and its transaction

				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					throw StandardException.newException(
                        SQLState.XACT_INTERNAL_TRANSACTION_EXCEPTION, error);
                }

				throwAway = true;


			} else {

				// If the severity is lower than a transaction error then do nothing.
				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					return;
                }
                 

				// If the session is going to disappear then we want to close this
				// transaction, not just abort it.
				if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
					throwAway = true;
			}
		} else {
			// some java* error, throw away the transaction.
			throwAway = true;
		}

		try {

			if (transaction != null) {
				// abort the transaction
				transaction.abort();
			}

		} catch (StandardException se) {
			// if we get an error during abort then shut the system down
			throwAway = true;

			// if the system was being shut down anyway, do nothing
			if ((se.getSeverity() <= ExceptionSeverity.SESSION_SEVERITY) &&
				(se.getSeverity() >= ((StandardException) error).getSeverity())) {

				throw StandardException.newException(SQLState.XACT_ABORT_EXCEPTION, se);
			}

		} finally {

			if (throwAway) {
				// xact close will pop this context out of the context
				// stack 
				transaction.close();
				transaction = null;
			}
		}

	}

	SpliceTransaction getTransaction() {
		return transaction;
	}

	HBaseStore getFactory() {
		return factory;
	}

	void substituteTransaction(SpliceTransaction newTran)
	{
		LOG.debug("substituteTransaction the old trans=(context="+transaction.getContextId()+",transName="+transaction.getTransactionName()
				+",status="+transaction.getTransactionStatus()+",id="+transaction.toString()+")");
		
		LOG.debug("substituteTransaction the old trans=(context="+newTran.getContextId()+",transName="+newTran.getTransactionName()
				+",status="+newTran.getTransactionStatus()+",id="+newTran.toString()+")");
		
		// disengage old tran from this xact context
		SpliceTransaction oldTran = (SpliceTransaction)transaction;
		if (oldTran.transContext == this)
			oldTran.transContext = null;

		// set up double link between new transaction and myself
		transaction = newTran;
		((SpliceTransaction)transaction).transContext = this;
	}

}
