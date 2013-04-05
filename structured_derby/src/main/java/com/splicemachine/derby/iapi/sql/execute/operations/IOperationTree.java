package com.splicemachine.derby.iapi.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;

public interface IOperationTree {
	public void traverse(SpliceOperation operation);
	public NoPutResultSet execute() throws StandardException;
}
