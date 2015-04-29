package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.*;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class BroadcastJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(BroadcastJoinOperation.class);
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
                                                                         StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                 activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                 optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException,
                                                    ClassNotFoundException {
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
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
    public SpliceOperation getLeftOperation() {
        return leftResultSet;
    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        PairDataSet<ExecRow,LocatedRow> leftDataSet = leftResultSet.getDataSet().keyBy(new Keyer<LocatedRow>(operationContext, leftHashKeys));
        PairDataSet<ExecRow,LocatedRow> rightDataSet = rightResultSet.getDataSet().keyBy(new Keyer<LocatedRow>(operationContext, rightHashKeys));
        if (isOuterJoin) {
            PairDataSet<ExecRow, Tuple2<LocatedRow, Optional<LocatedRow>>> joinedDataSet = leftDataSet.<LocatedRow>broadcastLeftOuterJoin(rightDataSet);
            return joinedDataSet.map(new OuterJoinFunction(operationContext, wasRightOuterJoin, getEmptyRow())).filter(new JoinRestrictionPredicateFunction(operationContext));
        }
        else {
            PairDataSet<ExecRow, Tuple2<LocatedRow, LocatedRow>> joinedDataSet = leftDataSet.<LocatedRow>broadcastJoin(rightDataSet);
            return joinedDataSet.map(new InnerJoinFunction(operationContext, wasRightOuterJoin)).filter(new JoinRestrictionPredicateFunction(operationContext));
        }
    }
}
