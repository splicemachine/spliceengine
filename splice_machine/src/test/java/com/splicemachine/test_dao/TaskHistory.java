package com.splicemachine.test_dao;

import com.google.common.collect.Maps;
import com.splicemachine.derby.metrics.OperationMetric;

import java.util.Map;

public class TaskHistory {

    private long statementId;
    private long operationId;
    private long taskId;
    private String host;
    private String region;

    private Map<OperationMetric, Object> operationMetricMap = Maps.newHashMap();

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public long getOperationId() {
        return operationId;
    }

    public void setOperationId(long operationId) {
        this.operationId = operationId;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public Map<OperationMetric, Object> getOperationMetricMap() {
        return operationMetricMap;
    }

    public void setOperationMetricMap(Map<OperationMetric, Object> operationMetricMap) {
        this.operationMetricMap = operationMetricMap;
    }

    @Override
    public String toString() {
        return "TaskHistory{" +
                "statementId=" + statementId +
                ", operationId=" + operationId +
                ", taskId=" + taskId +
                ", host='" + host + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
