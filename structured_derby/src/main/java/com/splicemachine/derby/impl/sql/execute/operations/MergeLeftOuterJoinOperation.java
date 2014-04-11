package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

/**
 * @author P Trolard
 *         Date: 10/04/2014
 */
public class MergeLeftOuterJoinOperation extends MergeJoinOperation {
    private static Logger LOG = Logger.getLogger(MergeLeftOuterJoinOperation.class);

    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;

    { isOuterJoin = true; }

    public MergeLeftOuterJoinOperation() { super();}

    public MergeLeftOuterJoinOperation(SpliceOperation leftResultSet,
                                       int leftNumCols,
                                       SpliceOperation rightResultSet,
                                       int rightNumCols,
                                       int leftHashKeyItem,
                                       int rightHashKeyItem,
                                       Activation activation,
                                       GeneratedMethod restriction,
                                       int resultSetNumber,
                                       GeneratedMethod emptyRowFun,
                                       boolean wasRightOuterJoin,
                                       boolean oneRowRightSide,
                                       boolean notExistsRightSide,
                                       double optimizerEstimatedRowCount,
                                       double optimizerEstimatedCost,
                                       String userSuppliedOptimizerOverrides) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, leftHashKeyItem, rightHashKeyItem,
                 activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                 optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG, "instantiate");
        emptyRowFunMethodName = (emptyRowFun == null) ? null : emptyRowFun.getMethodName();
        this.wasRightOuterJoin = wasRightOuterJoin;
        init(SpliceOperationContext.newContext(activation));
        recordConstructorTime();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        emptyRowFun = (emptyRowFunMethodName == null) ? null :
                          new SpliceMethod<ExecRow>(emptyRowFunMethodName,activation);
    }

    @Override
    public ExecRow getEmptyRow() throws StandardException {
        if (emptyRow == null){
            emptyRow = emptyRowFun.invoke();
        }
        return emptyRow;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "LeftOuter"+super.prettyPrint(indentLevel);
    }


}
