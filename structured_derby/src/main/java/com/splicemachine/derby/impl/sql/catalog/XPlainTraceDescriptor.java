package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.metrics.OperationMetric;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jyuan on 5/9/14.
 */
public class XPlainTraceDescriptor extends XPLAINBaseTableDescriptor {

    private static final String TABLE_NAME = "SYSXPLAIN_TRACE";
    private static final String STATEMENTID_NAME = "STATEMENTID";
    private static final String SEQUENCEID_NAME = "sequenceId";
    private static final String LEVEL_NAME = "level";
    private static final String RIGHT_CHILD_ID_NAME = "rightChild";

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    protected String[] getPrimaryKeys(){
        return new String[]{STATEMENTID_NAME, SEQUENCEID_NAME};
    }

    @Override
    protected SystemColumn[] buildColumnList() {
        OperationMetric[] metrics = OperationMetric.values();
        Arrays.sort(metrics, new Comparator<OperationMetric>() {
            @Override
            public int compare(OperationMetric o1, OperationMetric o2) {
                return o1.getPosition() - o2.getPosition();
            }
        });
        SystemColumn[] columns = new SystemColumn[metrics.length + 7];
        columns[0] = SystemColumnImpl.getColumn(STATEMENTID_NAME,Types.BIGINT,false);
        columns[1] = SystemColumnImpl.getColumn(SEQUENCEID_NAME,Types.INTEGER,false);
        columns[2] = SystemColumnImpl.getColumn(LEVEL_NAME,Types.INTEGER,false);
        columns[3] = SystemColumnImpl.getColumn(RIGHT_CHILD_ID_NAME,Types.INTEGER,true);
        columns[4] = SystemColumnImpl.getColumn("OPERATIONTYPE",Types.VARCHAR,true,32642);
        columns[5] = SystemColumnImpl.getColumn("HOST",Types.VARCHAR,true,32642);
        columns[6] = SystemColumnImpl.getColumn("REGION",Types.VARCHAR,true,32462);
        for(int i=0;i<metrics.length;i++){
            String name = metrics[i].name().replaceAll("_", "");
            columns[i+7] = SystemColumnImpl.getColumn(name, Types.BIGINT,true);
        }
        return columns;
    }
}
