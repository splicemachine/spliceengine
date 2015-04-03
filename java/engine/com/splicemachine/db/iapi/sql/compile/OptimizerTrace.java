package com.splicemachine.db.iapi.sql.compile;

/**
 * @author Scott Fines
 *         Date: 4/3/15
 */
public interface OptimizerTrace{
    void trace(OptimizerFlag flag, int intParam1,int intParam2, double doubleParam,Object objectParam1);
}
