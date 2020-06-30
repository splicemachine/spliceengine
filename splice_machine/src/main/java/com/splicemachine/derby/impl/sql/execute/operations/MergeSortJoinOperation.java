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

import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * MergeSortJoinOperation (HashJoin: TODO JLEACH Needs to be renamed)
 *
 * There are 6 different relational processing paths determined by the different valid combinations of these boolean
 * fields (getJoinType, antiJoin, hasRestriction).  For more detail on these paths please check out:
 *
 * @see com.splicemachine.derby.impl.sql.execute.operations.JoinOperation
 *
 * Before determining the different paths, each operation retrieves its left and right datasets and keys them by a Keyer Function.
 *
 * @see com.splicemachine.derby.iapi.sql.execute.SpliceOperation#getDataSet(com.splicemachine.derby.stream.iapi.DataSetProcessor)
 * @see com.splicemachine.derby.stream.iapi.DataSet
 * @see DataSet#keyBy(com.splicemachine.derby.stream.function.SpliceFunction)
 * @see com.splicemachine.derby.stream.function.KeyerFunction
 *
 * Once each dataset is keyed, the following logic is performed for the appropriate processing path.
 *
 * 1. (inner,join,no restriction)
 *     Flow:  leftDataSet -> hashJoin (rightDataSet) -> map (InnerJoinFunction)
 *
 * @see PairDataSet#hashJoin(PairDataSet, OperationContext)
 * @see com.splicemachine.derby.stream.function.InnerJoinFunction
 *
 * 2. (inner,join,restriction)
 *     Flow:  leftDataSet -> hashLeftOuterJoin (RightDataSet)
 *     -> map (OuterJoinPairFunction) - filter (JoinRestrictionPredicateFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#hashLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 * @see com.splicemachine.derby.stream.function.JoinRestrictionPredicateFunction
 *
 * 3. (inner,antijoin,no restriction)
 *     Flow:  leftDataSet -> subtractByKey (rightDataSet) -> map (AntiJoinFunction)
 *
 * @see PairDataSet#subtractByKey(PairDataSet, OperationContext)
 * @see com.splicemachine.derby.stream.function.AntiJoinFunction
 *
 * 4. (inner,antijoin,restriction)
 *     Flow:  leftDataSet -> hashLeftOuterJoin (rightDataSet) -> map (AntiJoinRestrictionFlatMapFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#hashLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.AntiJoinRestrictionFlatMapFunction
 *
 * 5. (outer,join,no restriction)
 *     Flow:  leftDataSet -> hashLeftOuterJoin (rightDataSet) -> map (OuterJoinPairFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#hashLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 *
 * 6. (outer,join,restriction)
 *     Flow:  leftDataSet -> hashLeftOuterJoin (rightDataSet) -> map (OuterJoinPairFunction)
 *     -> Filter (JoinRestrictionPredicateFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#hashLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 * @see com.splicemachine.derby.stream.function.JoinRestrictionPredicateFunction
 *
 *
 */
public class MergeSortJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(MergeSortJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected SpliceMethod<ExecRow> rightEmptyRowFun;
    protected ExecRow rightEmptyRow;
    protected static final String NAME = MergeSortJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

	public MergeSortJoinOperation() {
        super();
    }

    public MergeSortJoinOperation(SpliceOperation leftResultSet,
                                  int leftNumCols,
                                  SpliceOperation rightResultSet,
                                  int rightNumCols,
                                  int leftHashKeyItem,
                                  int rightHashKeyItem,
                                  Activation activation,
                                  GeneratedMethod restriction,
                                  int resultSetNumber,
                                  boolean oneRowRightSide,
                                  byte semiJoinType,
                                  boolean rightFromSSQ,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides,
                                  String sparkExpressionTreeAsString) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide, semiJoinType, rightFromSSQ,
                optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides,
                sparkExpressionTreeAsString);
        SpliceLogUtils.trace(LOG, "instantiate");
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        SpliceLogUtils.trace(LOG, "leftHashkeyItem=%d,rightHashKeyItem=%d", leftHashKeyItem, rightHashKeyItem);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        JoinUtils.getMergedRow(leftRow, rightRow, wasRightOuterJoin, mergedRow);
    }

    @Override
    public String toString(){
        return "MergeSort"+super.toString();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "MergeSortJoin:" + super.prettyPrint(indentLevel);
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext<JoinOperation> operationContext = dsp.<JoinOperation>createOperationContext(this);
        dsp.incrementOpDepth();
        boolean useNativeSparkJoin = (dsp.getType().equals(DataSetProcessor.Type.SPARK)   &&
                                      (restriction == null || hasSparkJoinPredicate()) &&
                                      !rightFromSSQ && !containsUnsafeSQLRealComparison());

        if (useNativeSparkJoin)
            dsp.finalizeTempOperationStrings();

        // Prepare Left
        DataSet<ExecRow> leftDataSet1 = leftResultSet.getDataSet(dsp)
                .map(new CloneFunction<>(operationContext));

       // operationContext.pushScopeForOp("Prepare Left Side");
        DataSet<ExecRow> leftDataSet2 =
            leftDataSet1.map(new CountJoinedLeftFunction(operationContext));
        if (joinType == JoinNode.INNERJOIN && !isAntiJoin())
            leftDataSet2 = leftDataSet2.filter(new InnerJoinNullFilterFunction(operationContext,leftHashKeys));

        dsp.finalizeTempOperationStrings();

        // Prepare Right
        DataSet<ExecRow> rightDataSet1 = rightResultSet.getDataSet(dsp).map(new CloneFunction<>(operationContext));
        DataSet<ExecRow> rightDataSet2 =
            rightDataSet1.map(new CountJoinedRightFunction(operationContext));
//        if (!getJoinType) Remove all nulls from the right side...
        if (joinType != JoinNode.FULLOUTERJOIN)
            rightDataSet2 = rightDataSet2.filter(new InnerJoinNullFilterFunction(operationContext,rightHashKeys));

        dsp.decrementOpDepth();
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "getDataSet Performing MergeSortJoin type=%s, antiJoin=%s, hasRestriction=%s",
                    getJoinTypeString(), isAntiJoin(), restriction != null);
        }
        rightDataSet1.map(new CountJoinedRightFunction(operationContext));
        DataSet<ExecRow> joined;

        if (useNativeSparkJoin){
            if (joinType == JoinNode.LEFTOUTERJOIN)
                joined = leftDataSet2.join(operationContext,rightDataSet2, DataSet.JoinType.LEFTOUTER,false);
            else if (joinType == JoinNode.FULLOUTERJOIN)
                joined = leftDataSet2.join(operationContext,rightDataSet2, DataSet.JoinType.FULLOUTER,false);
            else if (isAntiJoin())
                joined = leftDataSet2.join(operationContext,rightDataSet2, DataSet.JoinType.LEFTANTI,false);
            else if (isInclusionJoin())
                joined = leftDataSet2.join(operationContext,rightDataSet2, DataSet.JoinType.LEFTSEMI,false);
            else
                joined = leftDataSet2.join(operationContext,rightDataSet2, DataSet.JoinType.INNER,false);
        } else{

            PairDataSet<ExecRow, ExecRow> rightDataSet =
                    rightDataSet2.keyBy(new KeyerFunction(operationContext, rightHashKeys));
//            operationContext.popScope();
            PairDataSet<ExecRow,ExecRow> leftDataSet =
                    leftDataSet2.keyBy(new KeyerFunction<ExecRow,JoinOperation>(operationContext, leftHashKeys));

            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "getDataSet Performing MergeSortJoin type=%s, antiJoin=%s, rightFromSSQ=%s, hasRestriction=%s",
                        getJoinTypeString(), isAntiJoin(), rightFromSSQ, restriction != null);
            joined = getJoinedDataset(operationContext, leftDataSet, rightDataSet);
            handleSparkExplain(joined, leftDataSet2, rightDataSet2, dsp);
        }
            return joined;
                    //.map(new CountProducedFunction(operationContext), true);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private DataSet<ExecRow> getJoinedDataset(
        OperationContext operationContext,
        PairDataSet<ExecRow, ExecRow> leftDataSet,
        PairDataSet<ExecRow, ExecRow> rightDataSet) throws StandardException {

        if (joinType == JoinNode.LEFTOUTERJOIN) { // Left Outer Join
            return leftDataSet.cogroup(rightDataSet, "Cogroup Left and Right", operationContext)
                        .flatmap(new CogroupLeftOuterJoinRestrictionFlatMapFunction<SpliceOperation>(operationContext))
                        .map(new SetCurrentLocatedRowFunction<>(operationContext));
        } else if (joinType == JoinNode.FULLOUTERJOIN) {
            return leftDataSet.cogroup(rightDataSet, "Cogroup Left and Right", operationContext)
                    .flatmap(new CogroupFullOuterJoinRestrictionFlatMapFunction<SpliceOperation>(operationContext, leftHashKeys))
                    .map(new SetCurrentLocatedRowFunction<>(operationContext));
        }
        else {
            if (isAntiJoin()) { // antijoin
                if (restriction !=null) { // with restriction
                    return leftDataSet.cogroup(rightDataSet, "Cogroup Left and Right", operationContext).values(operationContext)
                        .flatMap(new CogroupAntiJoinRestrictionFlatMapFunction(operationContext));
                } else { // No Restriction
                    return leftDataSet.subtractByKey(rightDataSet, operationContext).values(operationContext)
                            .map(new AntiJoinFunction(operationContext));
                }
            } else { // Inner Join
                // if inclusion join or regular inner join with one matching row on right
                if (isOneRowRightSide()) {
                    return leftDataSet.cogroup(rightDataSet, "Cogroup Left and Right", operationContext).values(operationContext)
                        .flatMap(new CogroupInnerJoinRestrictionFlatMapFunction(operationContext));
                }
                if (restriction !=null) { // with restriction
                    return leftDataSet.hashJoin(rightDataSet, operationContext)
                            .map(new InnerJoinFunction<SpliceOperation>(operationContext))
                            .filter(new JoinRestrictionPredicateFunction(operationContext));
                } else { // No Restriction
                    return leftDataSet.hashJoin(rightDataSet, operationContext)
                            .map(new InnerJoinFunction<SpliceOperation>(operationContext));
                }
            }
        }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    @Override
    public int[] getLeftHashKeys() {
        return leftHashKeys;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    @Override
    public int[] getRightHashKeys() {
        return rightHashKeys;
    }
}
