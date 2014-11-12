package com.splicemachine.test_dao;

public class StatementHistory {

    private long statementId;
    private String host;
    private String username;
    private long transactionId;
    private String status;
    private String statementSql;
    private int totalJobCount;
    private int successfulJobs;
    private int failedJobs;
    private int canceledJobs;
    private long startTimeMs;
    private long stopTimeMs;

    public long getStatementId() {
        return statementId;
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatementSql() {
        return statementSql;
    }

    public void setStatementSql(String statementSql) {
        this.statementSql = statementSql;
    }

    public int getTotalJobCount() {
        return totalJobCount;
    }

    public void setTotalJobCount(int totalJobCount) {
        this.totalJobCount = totalJobCount;
    }

    public int getSuccessfulJobs() {
        return successfulJobs;
    }

    public void setSuccessfulJobs(int successfulJobs) {
        this.successfulJobs = successfulJobs;
    }

    public int getFailedJobs() {
        return failedJobs;
    }

    public void setFailedJobs(int failedJobs) {
        this.failedJobs = failedJobs;
    }

    public int getCanceledJobs() {
        return canceledJobs;
    }

    public void setCanceledJobs(int canceledJobs) {
        this.canceledJobs = canceledJobs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
    }

    public long getStopTimeMs() {
        return stopTimeMs;
    }

    public void setStopTimeMs(long stopTimeMs) {
        this.stopTimeMs = stopTimeMs;
    }

    @Override
    public String toString() {
        return "StatementHistory{" +
                "statementId=" + statementId +
                ", host='" + host + '\'' +
                ", username='" + username + '\'' +
                ", transactionId=" + transactionId +
                ", status='" + status + '\'' +
                ", statementSql='" + statementSql + '\'' +
                ", totalJobCount=" + totalJobCount +
                ", successfulJobs=" + successfulJobs +
                ", failedJobs=" + failedJobs +
                ", canceledJobs=" + canceledJobs +
                ", startTimeMs=" + startTimeMs +
                ", stopTimeMs=" + stopTimeMs +
                '}';
    }
}
