package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;

public class SpliceWarningCollector implements WarningCollector {
	private Activation activation;
	public SpliceWarningCollector(Activation activation) {
		this.activation = activation;
	}
	@Override
	public void addWarning(String warningState) throws StandardException {
		activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));				
	}
}
