package com.splicemachine.derby.iapi.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

public interface IOperationTree {
	public void traverse(SpliceOperation operation);
	public NoPutResultSet execute() throws StandardException;
}
