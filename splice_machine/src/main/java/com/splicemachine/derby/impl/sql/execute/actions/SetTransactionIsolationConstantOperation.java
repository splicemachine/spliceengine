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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;

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
	public void executeConstantAction( Activation activation ) throws StandardException {
		activation.getLanguageConnectionContext().setIsolationLevel(isolationLevel);
	}
}

