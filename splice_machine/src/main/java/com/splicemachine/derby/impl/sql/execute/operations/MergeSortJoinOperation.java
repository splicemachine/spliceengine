package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


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


    private Joiner joiner;

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
				recordConstructorTime();
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
        JoinUtils.getMergedRow(leftRow, rightRow, wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
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
        OperationContext operationContext = dsp.createOperationContext(this);
        PairDataSet<ExecRow,LocatedRow> leftDataSet = leftResultSet.getDataSet().keyBy(new Keyer<LocatedRow>(operationContext, leftHashKeys));
        PairDataSet<ExecRow,LocatedRow> rightDataSet = rightResultSet.getDataSet().keyBy(new Keyer<LocatedRow>(operationContext, rightHashKeys));
        if (isOuterJoin) {
            PairDataSet<ExecRow, Tuple2<LocatedRow, Optional<LocatedRow>>> joinedDataSet = leftDataSet.<LocatedRow>hashLeftOuterJoin(rightDataSet);
            return joinedDataSet.map(new OuterJoinFunction(operationContext, wasRightOuterJoin, getEmptyRow())).filter(new JoinRestrictionPredicateFunction(operationContext));
        }
        else {
            PairDataSet<ExecRow, Tuple2<LocatedRow, LocatedRow>> joinedDataSet = leftDataSet.<LocatedRow>hashJoin(rightDataSet);
            return joinedDataSet.map(new InnerJoinFunction(operationContext, wasRightOuterJoin)).filter(new JoinRestrictionPredicateFunction(operationContext));
        }
    }

}
