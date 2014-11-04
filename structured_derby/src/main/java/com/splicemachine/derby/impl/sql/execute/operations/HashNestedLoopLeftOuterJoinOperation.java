package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 7/25/14
 */
public class HashNestedLoopLeftOuterJoinOperation extends HashNestedLoopJoinOperation {

    private boolean seenRow;
    private int emptyRightRowsReturned;
    private GeneratedMethod emptyRowFun;

    public HashNestedLoopLeftOuterJoinOperation() {
    }

    public HashNestedLoopLeftOuterJoinOperation(SpliceOperation leftResultSet, int leftNumCols,
                                                SpliceOperation rightResultSet, int rightNumCols,
                                                int leftHashKeyItem, int rightHashKeyItem,
                                                Activation activation,
                                                GeneratedMethod restriction, int resultSetNumber,
                                                boolean oneRowRightSide,
                                                GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
                                                boolean notExistsRightSide,
                                                double optimizerEstimatedRowCount, double optimizerEstimatedCost,
                                                String userSuppliedOptimizerOverrides) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                leftHashKeyItem, rightHashKeyItem, activation, restriction,
                resultSetNumber, oneRowRightSide, emptyRowFun, wasRightOuterJoin,
                notExistsRightSide, optimizerEstimatedRowCount, optimizerEstimatedCost,
                userSuppliedOptimizerOverrides);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        emptyRightRowsReturned = 0;
        emptyRowFun = (emptyRowFunMethodName == null) ? null :
                context.getPreparedStatement().getActivationClass().getMethod(emptyRowFunMethodName);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return next(true,spliceRuntimeContext);
    }

    @Override
    protected boolean allowEmptyRow(ExecRow leftN) {
        seenRow=false;
        return true;
    }

    @Override
    protected ExecRow getEmptyRightRow() throws StandardException {
        if(seenRow){
            emptyRightRowsReturned++;
            return null;
        }
        rightRow = (ExecRow)emptyRowFun.invoke(activation);
        seenRow=true;
        return rightRow;
    }


    @Override
    protected void nonNullRight() {
        seenRow=true;
    }
}
