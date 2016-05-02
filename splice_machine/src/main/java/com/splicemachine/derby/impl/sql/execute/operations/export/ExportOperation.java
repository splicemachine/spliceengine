package com.splicemachine.derby.impl.sql.execute.operations.export;


import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.compile.ExportNode;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.OperationUtils;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;

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

    protected static List<NodeType> NODE_TYPES = ImmutableList.of(NodeType.REDUCE);

    private SpliceOperation source;
    private ResultColumnDescriptor[] sourceColumnDescriptors;
    private ExportParams exportParams;

    private ExecRow currentTemplate;

    protected static final String NAME = ExportOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}
    
    public ExportOperation() {
    }

    public ExportOperation(SpliceOperation source,
                           ResultColumnDescriptor[] sourceColumnDescriptors,
                           Activation activation,
                           int rsNumber,
                           String exportPath,
                           boolean compression,
                           int replicationCount,
                           String encoding,
                           String fieldSeparator,
                           String quoteCharacter) throws StandardException {
        super(activation, rsNumber, 0d, 0d);

        if (replicationCount <= 0 && replicationCount != ExportNode.DEFAULT_INT_VALUE) {
            throw StandardException.newException(SQLState.EXPORT_PARAMETER_IS_WRONG);
        }

        this.source = source;
        this.sourceColumnDescriptors = sourceColumnDescriptors;
        this.exportParams = new ExportParams(exportPath, compression, replicationCount, encoding, fieldSeparator, quoteCharacter);
        this.activation = activation;
        try {
            new ExportPermissionCheck(exportParams).verify();
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
        return NODE_TYPES;
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
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, runtimeContext);
        jobResults = rowProvider.shuffleRows(soi, OperationUtils.cleanupSubTasks(this));
        this.rowsSunk = TaskStats.sumTotalRowsWritten(jobResults.getJobStats().getTaskStats());
        new ExportFailedTaskCleanup().cleanup(this.jobResults, this.exportParams);
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

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return indent + "resultSetNumber:" + resultSetNumber + indent
                + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public String getOptimizerOverrides(){
        return source.getOptimizerOverrides();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    // - - - - - - - - - - - -
    // export only methods
    // - - - - - - - - - - - -

    public ExportParams getExportParams() {
        return exportParams;
    }

    public ResultColumnDescriptor[] getSourceResultColumnDescriptors() {
        return this.sourceColumnDescriptors;
    }

    // - - - - - - - - - - - -
    // serialization
    // - - - - - - - - - - - -

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        currentTemplate = (ExecRow) in.readObject();
        exportParams = (ExportParams) in.readObject();
        int srcColDescriptors = in.readInt();
        sourceColumnDescriptors = new ResultColumnDescriptor[srcColDescriptors];

        for (int i = 0; i < srcColDescriptors; i++) {
            sourceColumnDescriptors[i] = (ResultColumnDescriptor) in.readObject();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeObject(currentTemplate);
        out.writeObject(exportParams);
        out.writeInt(sourceColumnDescriptors.length);
        for (int i = 0; i < sourceColumnDescriptors.length; i++) {
            out.writeObject(sourceColumnDescriptors[i]);
        }
    }

}
