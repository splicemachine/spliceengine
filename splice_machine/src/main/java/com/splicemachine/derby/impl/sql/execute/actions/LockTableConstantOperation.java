package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;

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

