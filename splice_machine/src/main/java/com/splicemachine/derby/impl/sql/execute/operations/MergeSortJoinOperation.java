package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * MergeSortJoinOperation (HashJoin: TODO JLEACH Needs to be renamed)
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
 *     Flow:  leftDataSet -> hashJoin (rightDataSet) -> map (InnerJoinFunction)
 *
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#hashJoin(com.splicemachine.derby.stream.iapi.PairDataSet)
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
 * @see com.splicemachine.derby.stream.iapi.PairDataSet#subtractByKey(com.splicemachine.derby.stream.iapi.PairDataSet)
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
    public int emptyRightRowsReturned = 0;
    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;
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
                                  boolean notExistsRightSide,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG, "instantiate");
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        try {
                init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
                throw Exceptions.parseException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
        emptyRightRowsReturned = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
        out.writeInt(emptyRightRowsReturned);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        SpliceLogUtils.trace(LOG, "leftHashkeyItem=%d,rightHashKeyItem=%d", leftHashKeyItem, rightHashKeyItem);
        emptyRightRowsReturned = 0;
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
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext<JoinOperation> operationContext = dsp.<JoinOperation>createOperationContext(this);

        // Prepare Left

        DataSet<LocatedRow> leftDataSet1 = leftResultSet.getDataSet(dsp);

        operationContext.pushScopeForOp("Prepare Left Side");
        DataSet<LocatedRow> leftDataSet2 =
            leftDataSet1.map(new CountJoinedLeftFunction(operationContext));
        PairDataSet<ExecRow,LocatedRow> leftDataSet =
            leftDataSet2.keyBy(new KeyerFunction<LocatedRow,JoinOperation>(operationContext, leftHashKeys));
        operationContext.popScope();

        // Prepare Right

        DataSet<LocatedRow> rightDataSet1 = rightResultSet.getDataSet(dsp);

        operationContext.pushScopeForOp("Prepare Right Side");
        DataSet<LocatedRow> rightDataSet2 =
            rightDataSet1.map(new CountJoinedRightFunction(operationContext));
        PairDataSet<ExecRow,LocatedRow> rightDataSet =
            rightDataSet2.keyBy(new KeyerFunction(operationContext, rightHashKeys));
        operationContext.popScope();

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getDataSet Performing MergeSortJoin type=%s, antiJoin=%s, hasRestriction=%s",
                    isOuterJoin ? "outer" : "inner", notExistsRightSide, restriction != null);

        try {
            operationContext.pushScopeForOp("Perform Join");
            DataSet<LocatedRow> joined = getJoinedDataset(operationContext, leftDataSet, rightDataSet);
            return joined.map(new CountProducedFunction(operationContext), true);
        } finally {
            operationContext.popScope();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private DataSet<LocatedRow> getJoinedDataset(
        OperationContext operationContext,
        PairDataSet<ExecRow, LocatedRow> leftDataSet,
        PairDataSet<ExecRow, LocatedRow> rightDataSet) {

        if (isOuterJoin) { // Outer Join
            return leftDataSet.cogroup(rightDataSet, "Cogroup Left and Right")
                        .flatmap(new CogroupOuterJoinRestrictionFlatMapFunction<SpliceOperation>(operationContext))
                        .map(new SetCurrentLocatedRowFunction<SpliceOperation>(operationContext));
        }
        else {
            if (this.notExistsRightSide) { // antijoin
                if (restriction !=null) { // with restriction
                    return leftDataSet.<LocatedRow>cogroup(rightDataSet, "Cogroup Left and Right").values()
                        .flatMap(new CogroupAntiJoinRestrictionFlatMapFunction(operationContext));
                } else { // No Restriction
                    return leftDataSet.<LocatedRow>subtractByKey(rightDataSet)
                            .map(new AntiJoinFunction(operationContext));
                }
            } else { // Inner Join
                if (isOneRowRightSide()) {
                    return leftDataSet.<LocatedRow>cogroup(rightDataSet, "Cogroup Left and Right").values()
                        .flatMap(new CogroupInnerJoinRestrictionFlatMapFunction(operationContext));
                }
                if (restriction !=null) { // with restriction
                    return leftDataSet.hashJoin(rightDataSet)
                            .map(new InnerJoinFunction<SpliceOperation>(operationContext))
                            .filter(new JoinRestrictionPredicateFunction(operationContext));
                } else { // No Restriction
                    return leftDataSet.hashJoin(rightDataSet)
                            .map(new InnerJoinFunction<SpliceOperation>(operationContext));
                }
            }
        }
    }

    @Override
    public int[] getLeftHashKeys() {
        return leftHashKeys;
    }

    @Override
    public int[] getRightHashKeys() {
        return rightHashKeys;
    }
}
