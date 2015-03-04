package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;


/**
 *	This is a wrapper class which invokes the Execution-time logic for
 *	SET TRANSACTION statements. The real Execution-time logic lives inside the
 *	executeConstantAction() method of the Execution constant.
 *
 */

public class SetTransactionOperation extends MiscOperation
{
	/**
     * Construct a SetTransactionResultSet
	 *
	 *  @param activation		Describes run-time environment.
     */
	public SetTransactionOperation(Activation activation) throws StandardException
    {
		super(activation);
		recordConstructorTime(); 
	}

	/**
	 * Does this ResultSet cause a commit or rollback.
	 *
	 * @return Whether or not this ResultSet cause a commit or rollback.
	 */
	public boolean doesCommit()
	{
		return true;
	}
}
