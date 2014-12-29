package com.splicemachine.derby.impl.sql.execute.operations.export;


import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import org.supercsv.io.CsvListWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Invoked by SinkTask to perform the export on each node.
 */
public class ExportOperationSink implements OperationSink {

    private static final Logger LOG = Logger.getLogger(ExportOperationSink.class);

    private final ExportOperation operation;
    private final Timer totalTimer;
    private final ExportFile exportFile;
    private final ResultColumnDescriptor[] resultColumnDescriptors;

    public ExportOperationSink(ExportOperation exportOperation, byte[] taskId) throws IOException {
        this.operation = exportOperation;
        this.totalTimer = Metrics.newTimer();
        this.exportFile = new ExportFile(exportOperation.getExportParams(), taskId);
        this.resultColumnDescriptors = exportOperation.getSourceResultColumnDescriptors();
    }

    @Override
    public TaskStats sink(SpliceRuntimeContext spliceRuntimeContext) throws Exception {
        long rowsRead = 0;
        long rowsWritten = 0;

        try (ExportExecRowWriter rowWriter = initializeExecRowWriter()) {
            totalTimer.startTiming();
            ExecRow row;
            while ((row = operation.getNextSinkRow(spliceRuntimeContext)) != null) {
                SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);
                rowsRead++;
                rowWriter.writeRow(row, resultColumnDescriptors);
                rowsWritten++;
            }
            totalTimer.stopTiming();
        } catch (Exception e) {
            LOG.error("task side export error", e);
            /* This task failed, delete any partial export file. */
            boolean deleteResult = exportFile.delete();
            LOG.error("attempted to delete partial export file with result = " + deleteResult);
            return handleException(e);
        } finally {
            operation.close();

        }
        return new TaskStats(totalTimer.getTime().getWallClockTime(), rowsRead, rowsWritten, new boolean[0]);
    }

    private TaskStats handleException(Exception e) throws Exception {
        Throwable t = Throwables.getRootCause(e);
        if (t instanceof InterruptedException) {
            throw (InterruptedException) t;
        } else {
            throw e;
        }
    }

    private ExportExecRowWriter initializeExecRowWriter() throws IOException {
        OutputStream outputStream = exportFile.getOutputStream();
        CsvListWriter writer = new ExportCSVWriterBuilder().build(outputStream, operation.getExportParams());
        return new ExportExecRowWriter(writer);
    }

}
