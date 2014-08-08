package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.TaskHistoryDescriptor;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.metrics.Metrics;
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
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jyuan on 7/1/14.
 */
public class SYSTASKHISTORYRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSTASKHISTORY";
    private static final int SYSTASKHISTORY_COLUMN_COUNT = 48;
    private static final int STATEMENTID = 1;
    private static final int OPERATIONID = 2;
    private static final int TASKID = 3;
    private static final int HOST = 4;
    private static final int REGION = 5;
    private static final int BUFFERFILLRATIO = 48;

    private String[] uuids = {
            "1f148197-646b-4fd6-95f1-f713649b3a63",
            "1f148197-646b-4fd6-95f1-f713649b3a63"
    };

    public SYSTASKHISTORYRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(SYSTASKHISTORY_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long statementId = 0;
        long operationId = 0;
        long taskId = 0;
        String host = null;
        String region = null;
        long[] metrics = new long[OperationMetric.size];
        double bufferFillRatio = 0;

        if (td != null) {
            TaskHistoryDescriptor d = (TaskHistoryDescriptor)td;
            statementId = d.getStatementId();
            operationId = d.getOperationId();
            taskId = d.getTaskId();
            host = d.getHost();
            region = d.getRegion();
            metrics = d.getMetrics();
            bufferFillRatio = d.getBufferFillRatio();
        }

        ExecRow row = getExecutionFactory().getValueRow(SYSTASKHISTORY_COLUMN_COUNT);

        row.setColumn(STATEMENTID, new SQLLongint(statementId));
        row.setColumn(OPERATIONID, new SQLLongint(operationId));
        row.setColumn(TASKID, new SQLLongint(taskId));
        row.setColumn(HOST, new SQLVarchar(host));
        row.setColumn(REGION, new SQLVarchar(region));

        for (int i = 0; i < metrics.length; ++i) {
            row.setColumn(i+6, new SQLLongint(metrics[i]));
        }
        row.setColumn(BUFFERFILLRATIO, new SQLDouble(bufferFillRatio));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException{
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == SYSTASKHISTORY_COLUMN_COUNT,
                    "Wrong number of columns for a STATEMENTHISTORY row");
        }

        DataValueDescriptor col = row.getColumn(STATEMENTID);
        long statementId = col.getLong();

        col = row.getColumn(OPERATIONID);
        long operationId = col.getLong();

        col = row.getColumn(TASKID);
        long taskId = col.getLong();

        col = row.getColumn(HOST);
        String host = col.getString();

        col = row.getColumn(REGION);
        String region = col.getString();

        long[] metrics = new long[OperationMetric.size];
        for (int i = 0; i < OperationMetric.size; ++i) {
            col = row.getColumn(i + 6);
            long val = col.getLong();
            metrics[i] = val;
        }

        col = row.getColumn(BUFFERFILLRATIO);
        double bufferFillRatio = col.getDouble();

        return new TaskHistoryDescriptor(statementId, operationId, taskId, host, region, metrics, bufferFillRatio);
    }

    @Override
    public SystemColumn[] buildColumnList() {
        OperationMetric[] metrics = OperationMetric.values();
        Arrays.sort(metrics, new Comparator<OperationMetric>() {
            @Override
            public int compare(OperationMetric o1, OperationMetric o2) {
                return o1.getPosition() - o2.getPosition();
            }
        });
        SystemColumn[] columns = new SystemColumn[metrics.length+6];
        columns[0] = SystemColumnImpl.getColumn("STATEMENTID", Types.BIGINT, false);
        columns[1] = SystemColumnImpl.getColumn("OPERATIONID",Types.BIGINT,false);
        columns[2] = SystemColumnImpl.getColumn("TASKID",Types.BIGINT,false);
        columns[3] = SystemColumnImpl.getColumn("HOST",Types.VARCHAR,false,32642);
        columns[4] = SystemColumnImpl.getColumn("REGION",Types.VARCHAR,true,32462);
        for(int i=0;i<metrics.length;i++){
            String name = metrics[i].name().replaceAll("_", "");
            columns[i+5] = SystemColumnImpl.getColumn(name, Types.BIGINT,true);
        }
        columns[metrics.length+5] = SystemColumnImpl.getColumn("BUFFER_FILLRATIO",Types.DOUBLE,true);
        return columns;
    }
}
