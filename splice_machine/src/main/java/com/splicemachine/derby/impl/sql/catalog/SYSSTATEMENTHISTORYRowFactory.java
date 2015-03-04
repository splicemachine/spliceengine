package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.StatementHistoryDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * Created by jyuan on 6/30/14.
 */
public class SYSSTATEMENTHISTORYRowFactory extends CatalogRowFactory {


    private static final String   TABLENAME_STRING = "SYSSTATEMENTHISTORY";
    private static final int SYSSTATEMENTHISTORY_COLUMN_COUNT = 12;

    private static final int STATEMENTID = 1;
    private static final int HOST = 2;
    private static final int USERNAME = 3;
    private static final int TRANSACTIONID = 4;
    private static final int STATUS = 5;
    private static final int STATEMENTSQL = 6;
    private static final int TOTALJOBCOUNT = 7;
    private static final int SUCCESSFULJOBS = 8;
    private static final int FAILEDJOBS = 9;
    private static final int CANCELLEDJOBS = 10;
    private static final int STARTTIME = 11;
    private static final int STOPTIME = 12;

    private static String uuids[] = {
            "290bebb6-a302-4d37-abad-c412d327975e",
            "290bebb6-a302-4d37-abad-c412d327975e"
    };

    public SYSSTATEMENTHISTORYRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSSTATEMENTHISTORY_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException{
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(
                    row.nColumns()==SYSSTATEMENTHISTORY_COLUMN_COUNT,
                    "Wrong number of columns for a STATEMENTHISTORY row");
        }

        DataDescriptorGenerator ddg = dataDictionary.getDataDescriptorGenerator();

        DataValueDescriptor col = row.getColumn(STATEMENTID);
        long statementId = col.getLong();

        col = row.getColumn(HOST);
        String host = col.getString();

        col = row.getColumn(USERNAME);
        String userName = col.getString();

        col = row.getColumn(TRANSACTIONID);
        long transactionId = col.getLong();

        col = row.getColumn(STATUS);
        String status = col.getString();

        col = row.getColumn(STATEMENTSQL);
        String statementSql = col.getString();

        col = row.getColumn(TOTALJOBCOUNT);
        int totalJobCount = col.getInt();

        col = row.getColumn(SUCCESSFULJOBS);
        int successfulJobs = col.getInt();

        col = row.getColumn(FAILEDJOBS);
        int failedJobs = col.getInt();

        col = row.getColumn(CANCELLEDJOBS);
        int cancelledJobs = col.getInt();

        col = row.getColumn(STARTTIME);
        long startTime = col.getLong();

        col = row.getColumn(STOPTIME);
        long stopTime = col.getLong();

        return new StatementHistoryDescriptor(statementId, host, userName, transactionId, status, statementSql,
                totalJobCount, successfulJobs, failedJobs, cancelledJobs, startTime, stopTime);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long statementId = 0;
        String host = null;
        String userName = null;
        long txnId = 0;
        String status = null;
        String statementSql = null;
        int totalJobCount = 0;
        int successfulJobs = 0;
        int failedJobs = 0;
        int cancelledJobs = 0;
        long startTime = 0;
        long stopTime = 0;

        if (td != null) {
            StatementHistoryDescriptor sd = (StatementHistoryDescriptor) td;
            statementId = sd.getStatementId();
            host = sd.getHost();
            userName = sd.getUserName();
            txnId = sd.getTransactionId();
            status = sd.getStatus();
            statementSql = sd.getStatementSql();
            totalJobCount = sd.getTotalJobCount();
            successfulJobs = sd.getSuccessfulJobs();
            failedJobs = sd.getFailedJobs();
            cancelledJobs = sd.getCancelledJobs();
            startTime = sd.getStartTime();
            stopTime = sd.getStopTime();

        }

        ExecRow row = getExecutionFactory().getValueRow(SYSSTATEMENTHISTORY_COLUMN_COUNT);
        row.setColumn(STATEMENTID, new SQLLongint(statementId));
        row.setColumn(HOST, new SQLVarchar(host));
        row.setColumn(USERNAME, new SQLVarchar(userName));
        row.setColumn(TRANSACTIONID, new SQLLongint(txnId));
        row.setColumn(STATUS, new SQLVarchar(status));
        row.setColumn(STATEMENTSQL, new SQLVarchar(statementSql));
        row.setColumn(TOTALJOBCOUNT, new SQLInteger(totalJobCount));
        row.setColumn(SUCCESSFULJOBS, new SQLInteger(successfulJobs));
        row.setColumn(FAILEDJOBS, new SQLInteger(failedJobs));
        row.setColumn(CANCELLEDJOBS, new SQLInteger(cancelledJobs));
        row.setColumn(STARTTIME, new SQLLongint(startTime));
        row.setColumn(STOPTIME, new SQLLongint(stopTime));

        return row;
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("STATEMENTID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("HOST",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("USERNAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("TRANSACTIONID",Types.BIGINT,false),
                SystemColumnImpl.getColumn("STATUS",Types.VARCHAR,false,10),
                SystemColumnImpl.getColumn("STATEMENTSQL",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("TOTALJOBCOUNT",Types.INTEGER,true),
                SystemColumnImpl.getColumn("SUCCESSFULJOBS",Types.INTEGER,true),
                SystemColumnImpl.getColumn("FAILEDJOBS",Types.INTEGER,true),
                SystemColumnImpl.getColumn("CANCELLEDJOBS",Types.INTEGER,true),
                SystemColumnImpl.getColumn("STARTTIMEMS",Types.BIGINT,true),
                SystemColumnImpl.getColumn("STOPTIMEMS",Types.BIGINT,true),
        };
    }
}
