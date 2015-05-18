package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.tools.HostnameUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Trace Metric recording logic associated with TableOperationSink.
 */
class TableOperationSinkTrace {

    private static String HOSTNAME = HostnameUtil.getHostname();

    private final SinkingOperation operation;
    private final byte[] taskId;
    private final long statementId;
    private final long waitTimeNs;
    private final TxnView txn;

    public TableOperationSinkTrace(SinkingOperation operation, byte[] taskId, long statementId, long waitTimeNs, TxnView txn) {
        this.operation = operation;
        this.taskId = taskId;
        this.statementId = statementId;
        this.waitTimeNs = waitTimeNs;
        this.txn = txn;
    }

    public void recordTraceMetrics(SpliceRuntimeContext spliceRuntimeContext, Timer writeTimer, WriteStats writeStats) throws IOException {
        if (spliceRuntimeContext.shouldRecordTraceMetrics()) {
            long taskIdLong = taskId != null ? Bytes.toLong(taskId) : SpliceDriver.driver().getUUIDGenerator().nextUUID();
            List<OperationRuntimeStats> operationStats = OperationRuntimeStats.getOperationStats(operation,
                    taskIdLong, statementId, writeStats, writeTimer.getTime(), spliceRuntimeContext);
            XplainTaskReporter reporter = SpliceDriver.driver().getTaskReporter();
            for (OperationRuntimeStats operationStat : operationStats) {
                operationStat.addMetric(OperationMetric.TASK_QUEUE_WAIT_WALL_TIME, waitTimeNs);
                operationStat.setHostName(HOSTNAME);
                reporter.report(operationStat, this.txn);
            }
        }
    }

}
