package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

public interface BufferedAggregator {
	   public void initialize(ExecRow row) throws StandardException;
	   public void merge(ExecRow newRow) throws StandardException;
	   public boolean isInitialized();
	   public ExecRow finish() throws StandardException;
	}
