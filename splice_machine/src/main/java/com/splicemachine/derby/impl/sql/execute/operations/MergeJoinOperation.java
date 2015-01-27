package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

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
    Joiner joiner;
    private OperationResultSet ors;


    // for overriding
    protected boolean wasRightOuterJoin = false;
    private IOStandardIterator<ExecRow> rightRows;

    protected static final String NAME = MergeJoinOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
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
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }
    
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
    	super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
    	if (LOG.isDebugEnabled()) {
    		SpliceLogUtils.debug(LOG,"left hash keys {%s}",Arrays.toString(leftHashKeys));
    		SpliceLogUtils.debug(LOG,"right hash keys {%s}",Arrays.toString(rightHashKeys));
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
    public ExecRow nextRow(SpliceRuntimeContext ctx) throws StandardException, IOException {
        if (joiner == null) {
            // Upon first call, init up the joined rows source
            joiner = initJoiner(ctx);
            timer = ctx.newTimer();
            timer.startTiming();
        }

        ExecRow next = joiner.nextRow(ctx);
        setCurrentRow(next);
        if (next == null) {
            timer.tick(joiner.getLeftRowsSeen());
            ors.close();
            removeFromOperationChain();
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
        SpliceRuntimeContext<ExecRow> ctxWithOverride = spliceRuntimeContext.copy();
        ctxWithOverride.unMarkAsSink();
        if (firstLeft != null) {
            firstLeft = firstLeft.getClone();
            ctxWithOverride.addScanStartOverride(getKeyRow(firstLeft, leftHashKeys));
            leftPushBack.pushBack(firstLeft);
        }

        if (shouldRecordStats()) {
            addToOperationChain(spliceRuntimeContext, null, rightResultSet.getUniqueSequenceID());
        }
        ors = new OperationResultSet(activation,rightResultSet);
        ors.sinkOpen(spliceRuntimeContext.getTxn(),true);
        ors.executeScan(false,ctxWithOverride);
        SpliceNoPutResultSet resultSet = ors.getDelegate();
        rightRows = StandardIterators.ioIterator(resultSet);
        rightRows.open();
        IJoinRowsIterator<ExecRow> mergedRowSource = new MergeJoinRows(leftPushBack, rightRows, leftHashKeys, rightHashKeys);
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
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "updateStats");
        if (joiner != null) {
            long leftRowsSeen = joiner.getLeftRowsSeen();
            stats.addMetric(OperationMetric.INPUT_ROWS, leftRowsSeen);
            TimeView time = timer.getTime();
            stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
            stats.addMetric(OperationMetric.TOTAL_WALL_TIME,time.getWallClockTime());
            stats.addMetric(OperationMetric.TOTAL_CPU_TIME,time.getCpuTime());
            stats.addMetric(OperationMetric.TOTAL_USER_TIME, time.getUserTime());
            stats.addMetric(OperationMetric.FILTERED_ROWS, joiner.getRowsFiltered());
        }

        if (rightRows != null) {
            IOStats rightSideStats = rightRows.getStats();
            TimeView remoteView = rightSideStats.getTime();
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rightSideStats.getRows());
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,rightSideStats.getBytes());
        }
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "leftRows %d, rightRows %d, rowsFiltered=%d",joiner.getLeftRowsSeen(), joiner.getRightRowsSeen(),joiner.getRowsFiltered());

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
