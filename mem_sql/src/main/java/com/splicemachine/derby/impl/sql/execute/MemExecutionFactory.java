package com.splicemachine.derby.impl.sql.execute;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class MemExecutionFactory extends SpliceExecutionFactory{
    @Override
    protected SpliceGenericConstantActionFactory newConstantActionFactory(){
        return new MemGenericConstantActionFactory();
    }
}
