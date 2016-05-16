package com.splicemachine.derby.impl.sql.execute;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HExecutionFactory extends SpliceExecutionFactory{
    @Override
    protected SpliceGenericConstantActionFactory newConstantActionFactory(){
        return new HbaseGenericConstantActionFactory();
    }
}
