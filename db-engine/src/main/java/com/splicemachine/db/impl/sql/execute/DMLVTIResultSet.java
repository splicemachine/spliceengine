/*

   Derby - Class org.apache.derby.impl.sql.execute.DMLVTIResultSet

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

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;

import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * Base class for Insert, Delete & UpdateVTIResultSet
 */
abstract class DMLVTIResultSet extends DMLWriteResultSet
{

	// passed in at construction time

	NoPutResultSet sourceResultSet;
	NoPutResultSet savedSource;
	UpdatableVTIConstantAction	constants;
	TransactionController 	tc;

    ResultDescription 		resultDescription;
	private int						numOpens;
	boolean				firstExecute;

	/**
     * Returns the description of the inserted rows.
     * REVISIT: Do we want this to return NULL instead?
	 */
	public ResultDescription getResultDescription()
	{
	    return resultDescription;
	}

    /**
	 *
	 * @exception StandardException		Thrown on error
     */
    DMLVTIResultSet(NoPutResultSet source, 
						   Activation activation)
		throws StandardException
    {
		super(activation);
		sourceResultSet = source;
		constants = (UpdatableVTIConstantAction) constantAction;

        tc = activation.getTransactionController();

        resultDescription = sourceResultSet.getResultDescription();
	}
	
	/**
		@exception StandardException Standard Derby error policy
	*/
	public void open() throws StandardException
	{
		setup();
		// Remember if this is the 1st execution
		firstExecute = (numOpens == 0);

		rowCount = 0;

		if (numOpens++ == 0)
		{
			sourceResultSet.openCore();
		}
		else
		{
			sourceResultSet.reopenCore();
		}

        openCore();
       
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = sourceResultSet;
		}

		cleanUp();

		endTime = getCurrentTimeMillis();
	} // end of open()

    protected abstract void openCore() throws StandardException;

	/**
	 * @see com.splicemachine.db.iapi.sql.ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp() throws StandardException
	{
		/* Close down the source ResultSet tree */
        if( null != sourceResultSet)
            sourceResultSet.close();
		numOpens = 0;
		super.close();
	} // end of cleanUp

	public void finish() throws StandardException
    {

		sourceResultSet.finish();
		super.finish();
	} // end of finish
}
