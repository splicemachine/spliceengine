package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.UniqueTupleDescriptor;

/**
 * Created by jyuan on 6/30/14.
 */
public class StatementHistoryDescriptor extends TupleDescriptor{

    private long statementId;
    private String host;
    private String userName;
    private long transactionId;
    private String status;
    private String statementSql;
    private int totalJobCount;
    private int successfulJobs;
    private int failedJobs;
    private int cancelledJobs;
    private long startTime;
    private long stopTime;

    public StatementHistoryDescriptor(long statementId,
                                      String host,
                                      String userName,
                                      long transactionId,
                                      String status,
                                      String statementSql,
                                      int totalJobCount,
                                      int successfulJobs,
                                      int failedJobs,
                                      int cancelledJobs,
                                      long startTime,
                                      long stopTime) {
        this.statementId = statementId;
        this.host = host;
        this.userName = userName;
        this.transactionId = transactionId;
        this.status = status;
        this.statementSql = statementSql;
        this.totalJobCount = totalJobCount;
        this.successfulJobs = successfulJobs;
        this.failedJobs = failedJobs;
        this.cancelledJobs = cancelledJobs;
        this.startTime = startTime;
        this.stopTime= stopTime;
    }

    public long getStatementId() {
        return statementId;
    }

    public String getHost() {
        return host;
    }

    public String getUserName() {
        return userName;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getStatus () {
        return status;
    }

    public String getStatementSql() {
        return statementSql;
    }

    public int getTotalJobCount() {
        return totalJobCount;
    }

    public int getSuccessfulJobs() {
        return successfulJobs;
    }

    public int getFailedJobs() {
        return failedJobs;
    }

    public int getCancelledJobs () {
        return cancelledJobs;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    /*@Override
    public String getDescriptorType() {
        return null;
    }

    @Override
    public String getDescriptorName() {
        return null;
    }*/
}
