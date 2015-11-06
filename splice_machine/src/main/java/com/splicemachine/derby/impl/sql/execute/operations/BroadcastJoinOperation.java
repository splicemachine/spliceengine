package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.function.broadcast.BroadcastJoinFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.utils.SpliceLogUtils;
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
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        leftHashKeyItem=in.readInt();
        rightHashKeyItem=in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
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

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> leftDataSet = leftResultSet.getDataSet(dsp);
        DataSet<LocatedRow> rds = rightResultSet.getDataSet(dsp);

        operationContext.pushScope("Broadcast Join");
        leftDataSet = leftDataSet.map(new CountJoinedLeftFunction(operationContext));
        PairDataSet<ExecRow,LocatedRow> keyedLeftDataSet = leftDataSet.keyBy(new KeyerFunction<LocatedRow>(operationContext, leftHashKeys));
        PairDataSet<ExecRow,LocatedRow> rightDataSet = rds.map(new CountJoinedRightFunction(operationContext)).keyBy(new KeyerFunction<LocatedRow>(operationContext, rightHashKeys));
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getDataSet Performing MergeSortJoin type=%s, antiJoin=%s, hasRestriction=%s",
                    isOuterJoin ? "outer" : "inner", notExistsRightSide, restriction != null);
        DataSet<LocatedRow> result;
        if (isOuterJoin) { // Outer Join with and without restriction
                    result = keyedLeftDataSet.broadcastCogroup(rightDataSet)
                            .flatmap(new CogroupOuterJoinRestrictionFlatMapFunction<SpliceOperation>(operationContext))
                            .map(new SetCurrentLocatedRowFunction<SpliceOperation>(operationContext));
        }
        else {
            if (this.notExistsRightSide) { // antijoin
                if (restriction !=null) { // with restriction
                    result = keyedLeftDataSet.<LocatedRow>broadcastCogroup(rightDataSet)
                            .flatmap(new CogroupAntiJoinRestrictionFlatMapFunction(operationContext));
                } else { // No Restriction
                    result = keyedLeftDataSet.<LocatedRow>broadcastSubtractByKey(rightDataSet)
                            .map(new AntiJoinFunction(operationContext));
                }
            } else { // Inner Join

                if (isOneRowRightSide()) {
                    result = keyedLeftDataSet.<LocatedRow>broadcastCogroup(rightDataSet)
                            .flatmap(new CogroupInnerJoinRestrictionFlatMapFunction(operationContext));
                } else {
                    result = leftDataSet.mapPartitions(new BroadcastJoinFlatMapFunction(operationContext))
                            .map(new InnerJoinFunction<SpliceOperation>(operationContext));

                    if (restriction !=null) { // with restriction
                        result = result.filter(new JoinRestrictionPredicateFunction<SpliceOperation>(operationContext));
                    }
                }
            }
        }
        result = result.map(new CountProducedFunction(operationContext));
        operationContext.popScope();
        return result;
    }

    public int[] getRightHashKeys() {
        return rightHashKeys;
    }

    public int[] getLeftHashKeys() {
        return leftHashKeys;
    }
}