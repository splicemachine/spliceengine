package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

public interface BufferedAggregator {
	   public void initialize(ExecRow row) throws StandardException;
	   public void merge(ExecRow newRow) throws StandardException;
	   public boolean isInitialized();
	   public ExecRow finish() throws StandardException;
	}
