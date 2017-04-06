/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.BroadcastJoinFlatMapFunction;
import com.splicemachine.derby.stream.function.broadcast.CogroupBroadcastJoinFunction;
import com.splicemachine.derby.stream.function.broadcast.SubtractByKeyBroadcastJoinFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 *
 * BroadcastJoinOperation
 *
 * There are 6 different relational processing paths determined by the different valid combinations of these boolean
 * fields (isOuterJoin, antiJoin, hasRestriction).  For more detail on these paths please check out:
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
 *     Flow:  leftDataSet -> broadcastJoin (rightDataSet) -> map (InnerJoinFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.InnerJoinFunction
 *
 * 2. (inner,join,restriction)
 *     Flow:  leftDataSet -> broadcastLeftOuterJoin (RightDataSet)
 *     -> map (OuterJoinPairFunction) - filter (JoinRestrictionPredicateFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 * @see com.splicemachine.derby.stream.function.JoinRestrictionPredicateFunction
 *
 * 3. (inner,antijoin,no restriction)
 *     Flow:  leftDataSet -> broadcastSubtractByKey (rightDataSet) -> map (AntiJoinFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastSubtractByKey(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.AntiJoinFunction
 *
 * 4. (inner,antijoin,restriction)
 *     Flow:  leftDataSet -> broadcastLeftOuterJoin (rightDataSet) -> map (AntiJoinRestrictionFlatMapFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.AntiJoinRestrictionFlatMapFunction
 *
 * 5. (outer,join,no restriction)
 *     Flow:  leftDataSet -> broadcastLeftOuterJoin (rightDataSet) -> map (OuterJoinPairFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 *
 * 6. (outer,join,restriction)
 *     Flow:  leftDataSet -> broadcastLeftOuterJoin (rightDataSet) -> map (OuterJoinPairFunction)
 *     -> Filter (JoinRestrictionPredicateFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#broadcastLeftOuterJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
 * @see com.splicemachine.derby.stream.function.OuterJoinPairFunction
 * @see com.splicemachine.derby.stream.function.JoinRestrictionPredicateFunction
 *
 *
 */

public class BroadcastJoinOperation extends JoinOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(BroadcastJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected List<ExecRow> rights;
    protected long sequenceId;
    protected static final String NAME = BroadcastJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    public BroadcastJoinOperation() {
        super();
    }

    public BroadcastJoinOperation(SpliceOperation leftResultSet,
                                  int leftNumCols,
                                  SpliceOperation rightResultSet,
                                  int rightNumCols,
                                  int leftHashKeyItem,
                                  int rightHashKeyItem,
                                  Activation activation,
                                  GeneratedMethod restriction,
                                  int resultSetNumber,
                                  boolean oneRowRightSide,
                                  boolean notExistsRightSide,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides) throws
            StandardException{
        super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,
                activation,restriction,resultSetNumber,oneRowRightSide,notExistsRightSide,
                optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        this.leftHashKeyItem=leftHashKeyItem;
        this.rightHashKeyItem=rightHashKeyItem;
        this.sequenceId = Bytes.toLong(operationInformation.getUUIDGenerator().nextBytes());
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        leftHashKeyItem=in.readInt();
        rightHashKeyItem=in.readInt();
        sequenceId = in.readLong();
    }

    public long getSequenceId() {
        return sequenceId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
        out.writeLong(sequenceId);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return leftResultSet;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> leftDataSet = leftResultSet.getDataSet(dsp);
        DataSet<LocatedRow> rightDataSet = rightResultSet.getDataSet(dsp);

//        operationContext.pushScope();
        leftDataSet = leftDataSet.map(new CountJoinedLeftFunction(operationContext));
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getDataSet Performing BroadcastJoin type=%s, antiJoin=%s, hasRestriction=%s",
                isOuterJoin ? "outer" : "inner", notExistsRightSide, restriction != null);

        DataSet<LocatedRow> result;
        if (dsp.getType().equals(DataSetProcessor.Type.SPARK) &&
                (restriction ==null || (!isOuterJoin && !notExistsRightSide))) {
            if (isOuterJoin)
                result = leftDataSet.join(operationContext,rightDataSet, DataSet.JoinType.LEFTOUTER,true);
            else if (notExistsRightSide)
                result = leftDataSet.join(operationContext,rightDataSet, DataSet.JoinType.LEFTANTI,true);

            else { // Inner Join
                if (isOneRowRightSide()) {
                    result = leftDataSet.mapPartitions(new CogroupBroadcastJoinFunction(operationContext))
                            .flatMap(new InnerJoinRestrictionFlatMapFunction(operationContext));
                } else {
                    result = leftDataSet.join(operationContext,rightDataSet, DataSet.JoinType.INNER,true)
                            .filter(new JoinRestrictionPredicateFunction(operationContext));

                }
            }
        }
        else {
            if (isOuterJoin) { // Outer Join with and without restriction
                result = leftDataSet.mapPartitions(new CogroupBroadcastJoinFunction(operationContext))
                        .flatMap(new OuterJoinRestrictionFlatMapFunction<SpliceOperation>(operationContext))
                        .map(new SetCurrentLocatedRowFunction<SpliceOperation>(operationContext));
            } else {
                leftDataSet = leftDataSet.filter(new InnerJoinNullFilterFunction(operationContext,this.leftHashKeys));
                if (this.notExistsRightSide) { // antijoin
                    if (restriction != null) { // with restriction
                        result = leftDataSet.mapPartitions(new CogroupBroadcastJoinFunction(operationContext))
                                .flatMap(new AntiJoinRestrictionFlatMapFunction(operationContext));
                    } else { // No Restriction
                        result = leftDataSet.mapPartitions(new SubtractByKeyBroadcastJoinFunction(operationContext))
                                .map(new AntiJoinFunction(operationContext));
                    }
                } else { // Inner Join

                    if (isOneRowRightSide()) {
                        result = leftDataSet.mapPartitions(new CogroupBroadcastJoinFunction(operationContext))
                                .flatMap(new InnerJoinRestrictionFlatMapFunction(operationContext));
                    } else {
                        result = leftDataSet.mapPartitions(new BroadcastJoinFlatMapFunction(operationContext))
                                .map(new InnerJoinFunction<SpliceOperation>(operationContext));

                        if (restriction != null) { // with restriction
                            result = result.filter(new JoinRestrictionPredicateFunction(operationContext));
                        }
                    }
                }
            }
        }

        result = result.map(new CountProducedFunction(operationContext), /*isLast=*/true);

//        operationContext.popScope();

        return result;
    }

    public String getPrettyExplainPlan() {
        StringBuffer sb = new StringBuffer();
        sb.append(super.getPrettyExplainPlan());
        sb.append("\n\nBroadcast Join Right Side:\n\n");
        sb.append(getRightOperation() != null ? getRightOperation().getPrettyExplainPlan() : "");
        return sb.toString();
    }
    
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getRightHashKeys() {
        return rightHashKeys;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getLeftHashKeys() {
        return leftHashKeys;
    }
}
