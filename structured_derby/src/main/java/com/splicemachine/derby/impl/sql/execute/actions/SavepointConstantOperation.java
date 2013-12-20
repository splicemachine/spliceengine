package com.splicemachine.derby.impl.sql.execute.actions;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	Savepoint (rollback, release and set savepoint) Statement at Execution time.
 */

public class SavepointConstantOperation extends DDLConstantOperation {
	private final String savepointName; //name of the savepoint
	private final int	savepointStatementType; //Type of savepoint statement ie rollback, release or set savepoint
	/**
	 *	Make the ConstantAction for a set savepoint, rollback or release statement.
	 *
	 *  @param savepointName	Name of the savepoint.
	 *  @param savepointStatementType	set savepoint, rollback savepoint or release savepoint
	 */
	public SavepointConstantOperation(String savepointName, int savepointStatementType) {
		this.savepointName = savepointName;
		this.savepointStatementType = savepointStatementType;
	}

	// OBJECT METHODS
	public	String	toString() {
		if (savepointStatementType == 1)
			return constructToString("SAVEPOINT ", savepointName + " ON ROLLBACK RETAIN CURSORS ON ROLLBACK RETAIN LOCKS");
		else if (savepointStatementType == 2)
			return constructToString("ROLLBACK WORK TO SAVEPOINT ", savepointName);
		else
			return constructToString("RELEASE TO SAVEPOINT ", savepointName);
	}

	/**
	 *	This is the guts of the Execution-time logic for CREATE TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeConstantAction( Activation activation ) throws StandardException {
        throw StandardException.newException(MessageId.SPLICE_UNSUPPORTED_OPERATION, getClass().getSimpleName());
//		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
//			//Bug 4507 - savepoint not allowed inside trigger
//			StatementContext stmtCtxt = lcc.getStatementContext();
//			if (stmtCtxt!= null && stmtCtxt.inTrigger())
//				throw StandardException.newException(SQLState.NO_SAVEPOINT_IN_TRIGGER);
//		if (savepointStatementType == 1) { //this is set savepoint
//			if (savepointName.startsWith("SYS")) //to enforce DB2 restriction which is savepoint name can't start with SYS
//				throw StandardException.newException(SQLState.INVALID_SCHEMA_SYS, "SYS");
//			lcc.languageSetSavePoint(savepointName, savepointName);
//		} else if (savepointStatementType == 2) { //this is rollback savepoint
//			lcc.internalRollbackToSavepoint(savepointName,true, savepointName);
//		} else { //this is release savepoint
//			lcc.releaseSavePoint(savepointName, savepointName);
//		}
	}

}