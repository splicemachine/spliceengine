package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.si.impl.PushBackIterator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static com.splicemachine.derby.utils.StandardIterators.StandardIteratorIterator;

/**
 * @author P Trolard
 *         Date: 18/11/2013
 */
public class MergeJoinOperation extends JoinOperation {

    private static final Logger LOG = Logger.getLogger(MergeJoinOperation.class);

    static List<NodeType> nodeTypes = Arrays.asList(NodeType.MAP);
    private int leftHashKeyItem;
    private int rightHashKeyItem;
    int[] leftHashKeys;
    int[] rightHashKeys;
    IJoinRowsIterator<ExecRow> mergedRowSource;
    Joiner joiner;
    private StandardIteratorIterator<ExecRow> leftBridgeIterator;
    private StandardIteratorIterator<ExecRow> rightBridgeIterator;

    // for overriding
    protected boolean wasRightOuterJoin = false;

    public MergeJoinOperation() {
        super();
    }

    public MergeJoinOperation(SpliceOperation leftResultSet,
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
                              String userSuppliedOptimizerOverrides)
            throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide,
                notExistsRightSide, optimizerEstimatedRowCount,
                optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        if (leftHashKeys.length > 1) {
            throw new RuntimeException(
                    "MergeJoin cannot currently be used with more than one equijoin key.");
        }
        if (rightResultSet instanceof IndexRowToBaseRowOperation) {
            throw new RuntimeException(
                    "MergeJoin cannot currently be used with a non-covering index on its right side.");
        }
				startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (joiner == null) {
            // Upon first call, init up the joined rows source
            joiner = initJoiner(spliceRuntimeContext);
						timer = spliceRuntimeContext.newTimer();
        }

				timer.startTiming();
        ExecRow next = joiner.nextRow();
        setCurrentRow(next);
				if(next==null){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}else
					timer.tick(1);
        return next;
    }

    private Joiner initJoiner(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        leftBridgeIterator = StandardIterators.asIter(StandardIterators.wrap(new Callable<ExecRow>() {
            @Override
            public ExecRow call() throws Exception {
                return leftResultSet.nextRow(spliceRuntimeContext);
            }
        }));
        PushBackIterator<ExecRow> leftPushBack = new PushBackIterator<ExecRow>(leftBridgeIterator);
        if (leftPushBack.hasNext()) {
            ExecRow firstLeft = leftPushBack.next().getClone();
            spliceRuntimeContext.addScanStartOverride(getKeyRow(firstLeft, leftHashKeys[0]));
            leftPushBack.pushBack(firstLeft);
        }
        rightBridgeIterator = StandardIterators.asIter(StandardIterators.wrap(
                rightResultSet.executeScan(spliceRuntimeContext)));
        rightBridgeIterator.open();
        mergedRowSource = new MergeJoinRows(leftPushBack, rightBridgeIterator,
                leftHashKeys, rightHashKeys);
        return new Joiner(mergedRowSource, getExecRowDefinition(), wasRightOuterJoin,
                leftNumCols, rightNumCols, oneRowRightSide, notExistsRightSide, null);
    }

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				int leftRowsSeen = joiner.getLeftRowsSeen();
				int rightRowsSeen = joiner.getRightRowsSeen();
				stats.addMetric(OperationMetric.INPUT_ROWS, leftRowsSeen + rightRowsSeen);
				//filtered = left*right -output
				stats.addMetric(OperationMetric.FILTERED_ROWS,leftRowsSeen*rightRowsSeen-timer.getNumEvents());
				super.updateStats(stats);
		}

		private ExecRow getKeyRow(ExecRow row, int keyIdx) throws StandardException {
        ExecRow keyRow = activation.getExecutionFactory().getValueRow(1);
        keyRow.setColumn(1, row.getColumn(keyIdx + 1));
        return keyRow;
    }

    @Override
    public void close() throws StandardException, IOException {
        if (rightBridgeIterator != null) {
            rightBridgeIterator.close();
            leftBridgeIterator.close();
        }
        super.close();
    }

}
