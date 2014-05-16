package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.stats.IOStats;
import com.splicemachine.stats.TimeView;
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

    // for overriding
    protected boolean wasRightOuterJoin = false;
		private IOStandardIterator<ExecRow> rightRows;

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
        if (rightResultSet instanceof TableScanOperation) {
            TableScanOperation scan = (TableScanOperation) rightResultSet;
            SpliceConglomerate conglomerate = scan.scanInformation.getConglomerate();
            int[] columnOrdering = conglomerate.getColumnOrdering();
            List<Integer> keySortIndexes = Lists.newLinkedList();
            for (int i = 0; i < rightHashKeys.length; i++) {
                int col = columnOrdering[i];
                boolean found = false;
                for (int j = 0; j < rightHashKeys.length; j++) {
                    if (col == rightHashKeys[j]) {
                        found = true;
                        keySortIndexes.add(j);
                    }
                }
                if (!found) {
                    break;
                }
            }
            if (keySortIndexes.size() == 0) {
                throw new RuntimeException("Equijoin predicates for join do not contain" +
                                               " a reference to the primary sort column for the" +
                                               " right-hand side of the join, so a Merge join cannot" +
                                               " be executed.");
            }
            int[] leftSortedHashKeys = new int[keySortIndexes.size()];
            int[] rightSortedHashKeys = new int[keySortIndexes.size()];
            for (int i = 0; i < keySortIndexes.size(); i++) {
                leftSortedHashKeys[i] = leftHashKeys[keySortIndexes.get(i)];
                rightSortedHashKeys[i] = rightHashKeys[keySortIndexes.get(i)];
            }
            leftHashKeys = leftSortedHashKeys;
            rightHashKeys = rightSortedHashKeys;

        } else if (rightResultSet instanceof IndexRowToBaseRowOperation) {
            throw new RuntimeException("MergeJoin cannot currently be used with a non-covering index on its right side.");
        } else {
            throw new RuntimeException("MergeJoin currently can be used only with a simple table scan on its right side.");
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
            joiner.open();
            timer = spliceRuntimeContext.newTimer();
						timer.startTiming();
        }

        ExecRow next = joiner.nextRow();
        setCurrentRow(next);
        if (next == null) {
            timer.tick(joiner.getLeftRowsSeen());
            joiner.close();
            stopExecutionTime = System.currentTimeMillis();
        }
        return next;
    }

    private Joiner initJoiner(final SpliceRuntimeContext<ExecRow> spliceRuntimeContext)
            throws StandardException, IOException {
        StandardPushBackIterator<ExecRow> leftPushBack =
            new StandardPushBackIterator<ExecRow>(StandardIterators.wrap(leftResultSet));
        ExecRow firstLeft = leftPushBack.next(spliceRuntimeContext);
        if (firstLeft != null) {
            firstLeft = firstLeft.getClone();
            spliceRuntimeContext.addScanStartOverride(getKeyRow(firstLeft, leftHashKeys));
            leftPushBack.pushBack(firstLeft);
        }
        rightRows = StandardIterators
                                                  .ioIterator(rightResultSet.executeScan(spliceRuntimeContext));
        mergedRowSource = new MergeJoinRows(leftPushBack, rightRows, leftHashKeys, rightHashKeys);
        StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return getEmptyRow();
            }
        };

        return new Joiner(mergedRowSource, getExecRowDefinition(), getRestriction(),
                             isOuterJoin, wasRightOuterJoin, leftNumCols, rightNumCols,
                             oneRowRightSide, notExistsRightSide, emptyRowSupplier,spliceRuntimeContext);
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        long leftRowsSeen = joiner.getLeftRowsSeen();
        stats.addMetric(OperationMetric.INPUT_ROWS, leftRowsSeen);
				TimeView time = timer.getTime();
				stats.addMetric(OperationMetric.TOTAL_WALL_TIME,time.getWallClockTime());
				stats.addMetric(OperationMetric.TOTAL_CPU_TIME,time.getCpuTime());
				stats.addMetric(OperationMetric.TOTAL_USER_TIME, time.getUserTime());
        stats.addMetric(OperationMetric.FILTERED_ROWS, joiner.getRowsFiltered());

				IOStats rightSideStats = rightRows.getStats();
				TimeView remoteView = rightSideStats.getTime();
				stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
				stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rightSideStats.getRows());
				stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,rightSideStats.getBytes());
        super.updateStats(stats);
    }

    private ExecRow getKeyRow(ExecRow row, int[] keyIndexes) throws StandardException {
        ExecRow keyRow = activation.getExecutionFactory().getValueRow(keyIndexes.length);
        for (int i = 0; i < keyIndexes.length; i++) {
            keyRow.setColumn(i + 1, row.getColumn(keyIndexes[i] + 1));
        }
        return keyRow;
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (joiner != null) joiner.close();
    }

}
