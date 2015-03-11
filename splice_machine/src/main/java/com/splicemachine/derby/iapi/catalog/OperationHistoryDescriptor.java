package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * Created by jyuan on 6/30/14.
 */
public class OperationHistoryDescriptor extends TupleDescriptor {

    private long statementId;
    private long operationId;
    private String operationType;
    private long parentOperationId;
    private String info;
    private boolean isRightChildOp;
    private boolean isSink;
    private int jobCount;
    private int taskCount;
    private int failedTaskCount;

    public OperationHistoryDescriptor(long statementId,
                                      long operationId,
                                      String operationType,
                                      long parentOperationId,
                                      String info,
                                      boolean isRightChildOp,
                                      boolean isSink,
                                      int jobCount,
                                      int taskCount,
                                      int failedTaskCount) {
        this.statementId = statementId;
        this.operationId = operationId;
        this.operationType = operationType;
        this.parentOperationId = parentOperationId;
        this.info = info;
        this.isRightChildOp = isRightChildOp;
        this.isSink = isSink;
        this.jobCount = jobCount;
        this.taskCount = taskCount;
        this.failedTaskCount = failedTaskCount;
    }

    public long getStatementId() {
        return statementId;
    }

    public long getOperationId() {
        return operationId;
    }

    public String getOperationType() {
        return operationType;
    }

    public long getParentOperationId() {
        return parentOperationId;
    }

    public String getInfo() {
        return info;
    }

    public boolean isRightChildOp() {
        return isRightChildOp;
    }

    public boolean isSink() {
        return isSink;
    }

    public int getJobCount() {
        return jobCount;
    }

    public int getTaskCount() {
        return taskCount;
    }

    public int getFailedTaskCount() {
        return failedTaskCount;
    }
}
