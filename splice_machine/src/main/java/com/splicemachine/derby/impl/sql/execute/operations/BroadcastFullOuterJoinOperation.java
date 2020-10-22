/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.CogroupBroadcastJoinFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by yxia on 12/3/19.
 */
public class BroadcastFullOuterJoinOperation extends BroadcastJoinOperation {
    private static Logger LOG = Logger.getLogger(BroadcastFullOuterJoinOperation.class);
    protected SpliceMethod<ExecRow> rightEmptyRowFun;
    protected ExecRow rightEmptyRow;
    protected String leftEmptyRowFunMethodName;
    protected SpliceMethod<ExecRow> leftEmptyRowFun;
    protected ExecRow leftEmptyRow;
    OperationContext opContextForLeftJoin;
    OperationContext opContextForAntiJoin;

    public BroadcastFullOuterJoinOperation() {
        super();
    }

    public BroadcastFullOuterJoinOperation(
            SpliceOperation leftResultSet,
            int leftNumCols,
            SpliceOperation rightResultSet,
            int rightNumCols,
            int leftHashKeyItem,
            int rightHashKeyItem,
            boolean noCacheBroadcastJoinRight,
            Activation activation,
            GeneratedMethod restriction,
            int resultSetNumber,
            GeneratedMethod leftEmptyRowFun,
            GeneratedMethod rightEmptyRowFun,
            boolean wasRightOuterJoin,
            boolean oneRowRightSide,
            byte semiJoinType,
            boolean rightFromSSQ,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String sparkExpressionTreeAsString) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, leftHashKeyItem, rightHashKeyItem,  noCacheBroadcastJoinRight,
                activation, restriction, resultSetNumber, oneRowRightSide, semiJoinType, rightFromSSQ,
                optimizerEstimatedRowCount, optimizerEstimatedCost,userSuppliedOptimizerOverrides,
                sparkExpressionTreeAsString);
        SpliceLogUtils.trace(LOG, "instantiate");
        leftEmptyRowFunMethodName = (leftEmptyRowFun == null) ? null : leftEmptyRowFun.getMethodName();
        rightEmptyRowFunMethodName = (rightEmptyRowFun == null) ? null : rightEmptyRowFun.getMethodName();
        this.wasRightOuterJoin = wasRightOuterJoin;
        this.joinType = JoinNode.FULLOUTERJOIN;
        init();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        rightEmptyRowFun = (rightEmptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(rightEmptyRowFunMethodName,activation);
        leftEmptyRowFun = (leftEmptyRowFunMethodName == null) ? null : new SpliceMethod<ExecRow>(leftEmptyRowFunMethodName,activation);
    }

    @Override
    public ExecRow getRightEmptyRow() throws StandardException {
        if (rightEmptyRow == null)
            rightEmptyRow = rightEmptyRowFun.invoke();
        return rightEmptyRow;
    }

    @Override
    public ExecRow getLeftEmptyRow() throws StandardException {
        if (leftEmptyRow == null)
            leftEmptyRow = leftEmptyRowFun.invoke();
        return leftEmptyRow;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "FullOuter"+super.prettyPrint(indentLevel);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftEmptyRowFunMethodName = readNullableString(in);
        rightEmptyRowFunMethodName = readNullableString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        writeNullableString(leftEmptyRowFunMethodName, out);
        writeNullableString(rightEmptyRowFunMethodName, out);
    }

    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext operationContext = dsp.createOperationContext(this);

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getDataSet Performing BroadcastJoin type=%s, antiJoin=%s, hasRestriction=%s",
                    getJoinTypeString(), isAntiJoin(), restriction != null);

        DataSet<ExecRow> result;
        boolean usesNativeSparkDataSet =
                (dsp.getType().equals(DataSetProcessor.Type.SPARK) &&
                        (restriction == null || hasSparkJoinPredicate()) &&
                        !containsUnsafeSQLRealComparison());

        dsp.incrementOpDepth();
        if (usesNativeSparkDataSet)
            dsp.finalizeTempOperationStrings();

        DataSet<ExecRow> leftDataSet;
        DataSet<ExecRow> rightDataSet;
        if (usesNativeSparkDataSet)
        {
            opContextForLeftJoin = null;
            opContextForAntiJoin = null;
            leftDataSet = leftResultSet.getDataSet(dsp);
            leftDataSet = leftDataSet.map(new CountJoinedLeftFunction(operationContext));
            dsp.finalizeTempOperationStrings();
            rightDataSet = rightResultSet.getDataSet(dsp);
            result = leftDataSet.join(operationContext,rightDataSet, DataSet.JoinType.FULLOUTER,true);
        }
        else {
            try {
                // clone the context for the computation of the left join and anti-join
                opContextForLeftJoin = operationContext.getClone();
                opContextForAntiJoin = operationContext.getClone();

                // compute left join
                BroadcastJoinOperation opCloneForLeftJoin = (BroadcastJoinOperation) opContextForLeftJoin.getOperation();
                leftDataSet = opCloneForLeftJoin.getLeftOperation().getDataSet(dsp).map(new CountJoinedLeftFunction(opContextForLeftJoin));
                dsp.finalizeTempOperationStrings();
                result = leftDataSet.mapPartitions(new CogroupBroadcastJoinFunction(opContextForLeftJoin,noCacheBroadcastJoinRight))
                        .flatMap(new LeftOuterJoinRestrictionFlatMapFunction(opContextForLeftJoin));

                // do right anti join left to get the non-matching rows
                BroadcastJoinOperation opCloneForAntiJoin = (BroadcastJoinOperation) opContextForAntiJoin.getOperation();

                DataSet<ExecRow> nonMatchingRightSet = opCloneForAntiJoin.getRightResultSet().getDataSet(dsp).mapPartitions(new CogroupBroadcastJoinFunction(opContextForAntiJoin, true, noCacheBroadcastJoinRight))
                        .flatMap(new LeftAntiJoinRestrictionFlatMapFunction(opContextForAntiJoin, true));
                rightDataSet = nonMatchingRightSet;
                result = result.union(nonMatchingRightSet, operationContext)
                        .map(new SetCurrentLocatedRowFunction(operationContext));
            } catch (Exception e) {
                throw StandardException.plainWrapException(e);
            }
        }

        result = result.map(new CountProducedFunction(operationContext), /*isLast=*/false);
        dsp.decrementOpDepth();
        handleSparkExplain(result, leftDataSet, rightDataSet, dsp);

        return result;
    }

    @Override
    public void close() throws StandardException {
        super.close();
        if (opContextForLeftJoin != null)
            opContextForLeftJoin.getOperation().close();
        if (opContextForAntiJoin != null)
            opContextForAntiJoin.getOperation().close();
    }

}

