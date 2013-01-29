package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.log4j.Logger;


/**
 * This is a wrapper class which invokes the Execution-time logic for
 * Misc statements. The real Execution-time logic lives inside the
 * executeConstantAction() method. Note that when re-using the
 * language result set tree across executions (DERBY-827) it is not
 * possible to store the ConstantAction as a member variable, because
 * a re-prepare of the statement will invalidate the stored
 * ConstantAction. Re-preparing a statement does not create a new
 * Activation unless the GeneratedClass has changed, so the existing
 * result set tree may survive a re-prepare.
 */

public class MiscOperation extends NoRowsResultSetOperation
{
	private static Logger LOG = Logger.getLogger(MiscOperation.class);
	
	/**
     * Construct a MiscResultSet
	 *
	 *  @param activation		Describes run-time environment.
     */
	public MiscOperation(Activation activation)
    {
		super(activation);
	}
    
	/**
	 * Opens a MiscResultSet, executes the Activation's
	 * ConstantAction, and then immediately closes the MiscResultSet.
	 *
	 * @exception StandardException Standard Derby error policy.
	 */
	public void open() throws StandardException
	{
		setup();
		activation.getConstantAction().executeConstantAction(activation);
		close();
	}

	// Does not override close() (no action required)
	// Does not override finish() (no action required)

	/**
	 * No action is required, but not implemented in any base class
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
	}
}
