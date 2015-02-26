package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.OperationHistoryDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * Created by jyuan on 6/30/14.
 */
public class SYSOPERATIONHISTORYRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSOPERATIONHISTORY";
    private static final int SYSOPERATIONHISTORY_COLUMN_COUNT = 10;

    private static final int STATEMENTID = 1;
    private static final int OPERATIONID = 2;
    private static final int OPERATION_TYPE = 3;
    private static final int PARENT_OPERATION_ID = 4;
    private static final int INFO = 5;
    private static final int IS_RIGHT_CHILD_OP = 6;
    private static final int IS_SINK = 7;
    private static final int JOBCOUNT = 8;
    private static final int TASKCOUNT = 9;
    private static final int FAILEDTASKCOUNT = 10;

    private static String uuids[] = {
            "ec6de6c9-b049-492a-a9a8-0f11360fffeb",
            "ec6de6c9-b049-492a-a9a8-0f11360fffeb"
    };

    public SYSOPERATIONHISTORYRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSOPERATIONHISTORY_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == SYSOPERATIONHISTORY_COLUMN_COUNT,
                    "Wrong number of columns for a STATEMENTHISTORY row");
        }

        DataValueDescriptor col = row.getColumn(STATEMENTID);
        long statementId = col.getLong();

        col = row.getColumn(OPERATIONID);
        long operationId = col.getLong();

        col = row.getColumn(OPERATION_TYPE);
        String operationType = col.getString();

        col = row.getColumn(PARENT_OPERATION_ID);
        long parentOperationId = col.getLong();

        col = row.getColumn(INFO);
        String info = col.getString();

        col = row.getColumn(IS_RIGHT_CHILD_OP);
        boolean isRightChildOp = col.getBoolean();

        col = row.getColumn(IS_SINK);
        boolean isSink = col.getBoolean();

        col = row.getColumn(JOBCOUNT);
        int jobCount = col.getInt();

        col = row.getColumn(TASKCOUNT);
        int taskCount = col.getInt();

        col = row.getColumn(FAILEDTASKCOUNT);
        int failedTaskCount = col.getInt();

        return new OperationHistoryDescriptor(statementId, operationId, operationType, parentOperationId, info,
                isRightChildOp, isSink, jobCount, taskCount, failedTaskCount);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {
        long statementId = 0;
        long operationId = 0;
        String operationType = null;
        long parentOperationId = 0;
        String info = null;
        boolean isRightOp = false;
        boolean isSink = false;
        int jobCount = 0;
        int taskCount = 0;
        int failedTaskCount = 0;

        if (td != null) {
            OperationHistoryDescriptor od = (OperationHistoryDescriptor)td;
            statementId = od.getStatementId();
            operationId = od.getOperationId();
            operationType = od.getOperationType();
            parentOperationId = od.getParentOperationId();
            info = od.getInfo();
            isRightOp = od.isRightChildOp();
            isSink = od.isSink();
            jobCount = od.getJobCount();
            taskCount = od.getTaskCount();
            failedTaskCount = od.getFailedTaskCount();
        }

        ExecRow row = getExecutionFactory().getValueRow(SYSOPERATIONHISTORY_COLUMN_COUNT);
        row.setColumn(STATEMENTID, new SQLLongint(statementId));
        row.setColumn(OPERATIONID, new SQLLongint(operationId));
        row.setColumn(OPERATION_TYPE, new SQLVarchar(operationType));
        row.setColumn(PARENT_OPERATION_ID, new SQLLongint(parentOperationId));
        row.setColumn(INFO, new SQLVarchar(info));
        row.setColumn(IS_RIGHT_CHILD_OP, new SQLBoolean(isRightOp));
        row.setColumn(IS_SINK, new SQLBoolean(isSink));
        row.setColumn(JOBCOUNT, new SQLInteger(jobCount));
        row.setColumn(TASKCOUNT, new SQLInteger(taskCount));
        row.setColumn(FAILEDTASKCOUNT, new SQLInteger(failedTaskCount));

        return row;
    }

    @Override
    public SystemColumn[] buildColumnList() {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("STATEMENTID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("OPERATIONID",Types.BIGINT,false),
                SystemColumnImpl.getColumn("OPERATION_TYPE",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("PARENT_OPERATION_ID",Types.BIGINT,true),
                SystemColumnImpl.getColumn("INFO",Types.VARCHAR,true,32642),
                SystemColumnImpl.getColumn("IS_RIGHT_CHILD_OP",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("IS_SINK",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("JOBCOUNT",Types.INTEGER,true),
                SystemColumnImpl.getColumn("TASKCOUNT",Types.INTEGER,true),
                SystemColumnImpl.getColumn("FAILEDTASKCOUNT",Types.INTEGER,true),
        };
    }
}
