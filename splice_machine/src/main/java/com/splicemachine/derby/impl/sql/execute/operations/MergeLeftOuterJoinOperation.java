/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * @author P Trolard
 *         Date: 10/04/2014
 */
public class MergeLeftOuterJoinOperation extends MergeJoinOperation {
    private static Logger LOG = Logger.getLogger(MergeLeftOuterJoinOperation.class);

    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;

    { joinType = JoinNode.LEFTOUTERJOIN; }

    public MergeLeftOuterJoinOperation() { super();}

    public MergeLeftOuterJoinOperation(SpliceOperation leftResultSet,
                                       int leftNumCols,
                                       SpliceOperation rightResultSet,
                                       int rightNumCols,
                                       int leftHashKeyItem,
                                       int rightHashKeyItem,
                                       int rightHashKeyToBaseTableMapItem,
                                       int rightHashKeySortOrderItem,
                                       Activation activation,
                                       GeneratedMethod restriction,
                                       int resultSetNumber,
                                       GeneratedMethod emptyRowFun,
                                       boolean wasRightOuterJoin,
                                       boolean oneRowRightSide,
                                       boolean notExistsRightSide,
                                       boolean rightFromSSQ,
                                       double optimizerEstimatedRowCount,
                                       double optimizerEstimatedCost,
                                       String userSuppliedOptimizerOverrides,
                                       String sparkExpressionTreeAsString) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, leftHashKeyItem, rightHashKeyItem,
                 rightHashKeyToBaseTableMapItem, rightHashKeySortOrderItem,
                 activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide, rightFromSSQ,
                 optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides,
                 sparkExpressionTreeAsString);
        SpliceLogUtils.trace(LOG, "instantiate");
        rightEmptyRowFunMethodName = (emptyRowFun == null) ? null : emptyRowFun.getMethodName();
        this.wasRightOuterJoin = wasRightOuterJoin;
        init();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        emptyRowFun = (rightEmptyRowFunMethodName == null) ? null :
                          new SpliceMethod<ExecRow>(rightEmptyRowFunMethodName,activation);
    }

    @Override
    public ExecRow getRightEmptyRow() throws StandardException {
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
