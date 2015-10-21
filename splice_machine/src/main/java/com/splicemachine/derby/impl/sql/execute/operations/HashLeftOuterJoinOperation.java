package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

public class HashLeftOuterJoinOperation extends NestedLoopLeftOuterJoinOperation {
    private static Logger LOG = Logger.getLogger(HashLeftOuterJoinOperation.class);

    public HashLeftOuterJoinOperation() {
        super();
    }

    public HashLeftOuterJoinOperation(
            SpliceOperation leftResultSet,
            int leftNumCols,
            SpliceOperation rightResultSet,
            int rightNumCols,
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
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, activation, restriction, resultSetNumber, emptyRowFun, wasRightOuterJoin,
                oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.isHash = true;
        SpliceLogUtils.trace(LOG, "instantiate");
        recordConstructorTime();
    }

}
