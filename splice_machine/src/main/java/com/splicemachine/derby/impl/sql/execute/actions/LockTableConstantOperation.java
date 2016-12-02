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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;

/**
 *	This class describes actions that are ALWAYS performed for a
 *	LOCK TABLE Statement at Execution time.
 *
 */

public class LockTableConstantOperation implements ConstantAction {
	private final String fullTableName;
	private final long conglomerateNumber;
	private final boolean exclusiveMode;
	
	/**
	 * Make the ConstantAction for a LOCK TABLE statement.
	 *
	 *  @param fullTableName		Full name of the table.
	 *  @param conglomerateNumber	Conglomerate number for the heap
	 *  @param exclusiveMode		Whether or not to get an exclusive lock.
	 */
	public LockTableConstantOperation(String fullTableName, long conglomerateNumber, boolean exclusiveMode) {
		this.fullTableName = fullTableName;
		this.conglomerateNumber = conglomerateNumber;
		this.exclusiveMode = exclusiveMode;
	}

	public	String	toString() {
		return "LOCK TABLE " + fullTableName;
	}

	/**
	 *	This is the guts of the Execution-time logic for LOCK TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
		throw StandardException.newException(SQLState.LANG_CANT_LOCK_TABLE, fullTableName, (exclusiveMode) ? "EXCLUSIVE" : "SHARE");
	}
}

