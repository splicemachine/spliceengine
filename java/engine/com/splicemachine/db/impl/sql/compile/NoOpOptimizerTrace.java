package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.sql.compile.OptimizerFlag;
import com.splicemachine.db.iapi.sql.compile.OptimizerTrace;

/**
 * @author Scott Fines
 *         Date: 4/3/15
 */
public class NoOpOptimizerTrace implements OptimizerTrace{
    public static final OptimizerTrace INSTANCE=  new NoOpOptimizerTrace();

    private NoOpOptimizerTrace(){}

    //no-op
    @Override public void trace(OptimizerFlag flag,int intParam1,int intParam2,double doubleParam,Object objectParam1){  }
}
