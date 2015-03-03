package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * Created by jyuan on 7/1/14.
 */
public class TaskHistoryDescriptor extends TupleDescriptor {

    private long statementId;
    private long operationId;
    private long taskId;
    private String host;
    private String region;
    private long[] metrics;
    private double bufferFillRatio;

    public TaskHistoryDescriptor(long statementId, long operationId, long taskId,
                                 String host, String region, long[] metrics,
                                 double bufferFillRatio) {
        this.statementId = statementId;
        this.operationId = operationId;
        this.taskId = taskId;
        this.host = host;
        this.region = region;
        this.metrics = metrics;
        this.bufferFillRatio = bufferFillRatio;
    }

    public long getStatementId() {
        return statementId;
    }

    public long getOperationId() {
        return operationId;
    }

    public long getTaskId() {
        return taskId;
    }

    public String getHost() {
        return host;
    }

    public String getRegion() {
        return region;
    }

    public long[] getMetrics() {
        return metrics;
    }

    public double getBufferFillRatio() {
        return bufferFillRatio;
    }
}
