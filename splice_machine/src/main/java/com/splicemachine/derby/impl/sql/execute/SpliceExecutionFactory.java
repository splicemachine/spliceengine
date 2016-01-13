package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.sql.execute.ResultSetFactory;
import com.splicemachine.db.impl.sql.execute.GenericConstantActionFactory;
import com.splicemachine.db.impl.sql.execute.GenericExecutionFactory;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceExecutionFactory extends GenericExecutionFactory {

    private static Logger LOG = Logger.getLogger(SpliceExecutionFactory.class);

    private SpliceGenericResultSetFactory resultSetFactory;
    private SpliceGenericConstantActionFactory genericConstantActionFactory;

    public SpliceExecutionFactory() {
        super();
        SpliceLogUtils.trace(LOG, "instantiating ExecutionFactory");
    }

    @Override
    public ResultSetFactory getResultSetFactory() {
        SpliceLogUtils.trace(LOG, "getResultSetFactory");
        if (resultSetFactory == null)
            resultSetFactory = new SpliceGenericResultSetFactory();
        return resultSetFactory;
    }

    @Override
    public GenericConstantActionFactory getConstantActionFactory() {
        if (genericConstantActionFactory == null) {
            genericConstantActionFactory = newConstantActionFactory();
        }
        return genericConstantActionFactory;
    }

    protected abstract SpliceGenericConstantActionFactory newConstantActionFactory();
}