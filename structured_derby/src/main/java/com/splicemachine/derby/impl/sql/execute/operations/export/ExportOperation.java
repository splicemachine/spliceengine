package com.splicemachine.derby.impl.sql.execute.operations.export;


import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.OperationUtils;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.si.api.TxnView;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.execute.ValueRow;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * Export the results of an arbitrary SELECT query to HDFS.
 */
public class ExportOperation extends SpliceBaseOperation implements SinkingOperation {

    private static final long serialVersionUID = 0L;

    protected static List<NodeType> PARALLEL_NODE_TYPES = ImmutableList.of(NodeType.REDUCE);

    private SpliceOperation source;
    private ExportParams exportParams;

    private ExecRow currentTemplate;

    public ExportOperation() {
    }

    public ExportOperation(SpliceOperation source,
                           Activation activation,
                           int rsNumber,
                           String exportPath,
                           String fileSystemType,
                           int replicationCount,
                           String encoding,
                           String fieldSeparator,
                           String quoteCharacter
    ) throws StandardException {
        super(activation, rsNumber, 0d, 0d);
        this.source = source;
        this.exportParams = new ExportParams(exportPath, fileSystemType, replicationCount, encoding, fieldSeparator, quoteCharacter);
        this.activation = activation;
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        currentTemplate = new ValueRow(0);
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return PARALLEL_NODE_TYPES;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.singletonList(source);
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        long totalTimeMillis = System.currentTimeMillis() - this.startExecutionTime;
        ExportRowProvider rowProvider = new ExportRowProvider(runtimeContext, this.rowsSunk, totalTimeMillis);
        return new SpliceNoPutResultSet(activation, this, rowProvider, true);
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
        return source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        long start = System.currentTimeMillis();
        PairDecoder pairDecoder = OperationUtils.getPairDecoder(this, runtimeContext);
        RowProvider rowProvider = getMapRowProvider(this, pairDecoder, runtimeContext);
        nextTime += System.currentTimeMillis() - start;
        TxnView txnView = operationInformation.getTransaction();
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext, txnView);
        jobResults = rowProvider.shuffleRows(soi, OperationUtils.cleanupSubTasks(this));
        this.rowsSunk = TaskStats.merge(jobResults.getJobStats().getTaskStats()).getTotalRowsWritten();
        return jobResults;
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if (source != null) {
            source.open();
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        source.close();
    }

    @Override
    public byte[] getUniqueSequenceId() {
        return uniqueSequenceID;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentTemplate;
    }

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (timer == null) {
            timer = spliceRuntimeContext.newTimer();
        }

        timer.startTiming();
        ExecRow row = source.nextRow(spliceRuntimeContext);
        if (row != null) {
            timer.tick(1);
            currentRow = row;
        } else {
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        }
        return row;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        throw new UnsupportedOperationException("Export Operation does not produce rows.");
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        if (timer != null) {
            stats.addMetric(OperationMetric.INPUT_ROWS, timer.getNumEvents());
        }
    }

    public SpliceOperation getSource() {
        return this.source;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return indent + "resultSetNumber:" + resultSetNumber + indent
                + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    public ExportParams getExportParams() {
        return exportParams;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        currentTemplate = (ExecRow) in.readObject();
        exportParams = (ExportParams) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeObject(currentTemplate);
        out.writeObject(exportParams);
    }

}
