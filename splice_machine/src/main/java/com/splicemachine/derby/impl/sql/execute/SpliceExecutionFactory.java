package com.splicemachine.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;
import org.apache.derby.impl.sql.execute.GenericConstantActionFactory;
import org.apache.derby.impl.sql.execute.GenericExecutionFactory;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceExecutionFactory extends GenericExecutionFactory {
	private static Logger LOG = Logger.getLogger(SpliceExecutionFactory.class);
	private SpliceGenericResultSetFactory resultSetFactory;
	private SpliceRealResultSetStatisticsFactory resultSetStatisticsFactory;
	public SpliceExecutionFactory() {
		super();
		SpliceLogUtils.trace(LOG,"instantiating ExecutionFactory");
	}
	@Override
	public ResultSetFactory getResultSetFactory() {
		SpliceLogUtils.trace(LOG,"getResultSetFactory");
		if (resultSetFactory == null)
			resultSetFactory = new SpliceGenericResultSetFactory();
		return resultSetFactory;
	}
	
	@Override
	public ResultSetStatisticsFactory getResultSetStatisticsFactory() throws StandardException {
		SpliceLogUtils.trace(LOG,"getResultSetStatisticsFactory");
		if (resultSetStatisticsFactory == null)
			resultSetStatisticsFactory = new SpliceRealResultSetStatisticsFactory();
		return resultSetStatisticsFactory;
	}

    @Override
    public GenericConstantActionFactory getConstantActionFactory() {
        if(genericConstantActionFactory == null){
            genericConstantActionFactory = new SpliceGenericConstantActionFactory();
        }
        return genericConstantActionFactory;
    }
}