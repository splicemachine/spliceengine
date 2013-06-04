package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	SET TRANSACTION ISOLATION Statement at Execution time.
 *
 */

public class SetTransactionIsolationConstantOperation implements ConstantAction {
	private final int isolationLevel;
	/**
	 *	Make the ConstantAction for a SET TRANSACTION ISOLATION statement.
	 *
	 *  @param isolationLevel	The new isolation level
	 */
	public SetTransactionIsolationConstantOperation(int isolationLevel) {
		this.isolationLevel = isolationLevel;
	}
	@Override
	public	String	toString() {
		return "SET TRANSACTION ISOLATION LEVEL = " + isolationLevel;
	}
	
	/**
	 *	This is the guts of the Execution-time logic for SET TRANSACTION ISOLATION.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	@Override
	public void	executeConstantAction( Activation activation ) throws StandardException {
		activation.getLanguageConnectionContext().setIsolationLevel(isolationLevel);
	}
}

