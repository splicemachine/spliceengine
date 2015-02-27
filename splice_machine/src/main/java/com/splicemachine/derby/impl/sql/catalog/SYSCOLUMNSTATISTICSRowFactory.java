package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.ColumnStatsDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;
import org.apache.derby.impl.sql.execute.ValueRow;

import java.sql.Types;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class SYSCOLUMNSTATISTICSRowFactory extends CatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSCOLUMNSTATISTICS";
    private static final int SYSCOLUMNSTATISTICS_COLUMN_COUNT = 4;
    private static final int CONGLOMID      = 1;
    private static final int PARTITIONID    = 2;
    private static final int COLUMNID       = 3;
    private static final int DATA           = 4;


    private String[] uuids = {
            "08264012-014b-c29c-0d2e-000003009390",
            "08264012-014b-c29c-0d2e-000003009390"
    };
    public SYSCOLUMNSTATISTICSRowFactory(UUIDFactory uuidFactory, ExecutionFactory exFactory, DataValueFactory dvf) {
        super(uuidFactory,exFactory,dvf);
        initInfo(SYSCOLUMNSTATISTICS_COLUMN_COUNT,TABLENAME_STRING,null,null,uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        ExecRow row = new ValueRow(SYSCOLUMNSTATISTICS_COLUMN_COUNT);
        row.setColumn(CONGLOMID,new SQLLongint());
        row.setColumn(PARTITIONID,new SQLVarchar());
        row.setColumn(COLUMNID,new SQLInteger());
        row.setColumn(DATA,new UserType());

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {
        DataValueDescriptor col = row.getColumn(CONGLOMID);
        long conglomId = col.getLong();
        col = row.getColumn(PARTITIONID);
        String partitionId = col.getString();
        col = row.getColumn(COLUMNID);
        int colNum = col.getInt();
        col = row.getColumn(DATA);
        ColumnStatistics colStats = (ColumnStatistics)col.getObject();

        return new ColumnStatsDescriptor(conglomId,
                partitionId,
                colNum,
                colStats.nullCount(),
                colStats.cardinality(),
                colStats.topK(),
                null,
                colStats.minValue(),
                0l,
                colStats.maxValue(),
                0l);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("CONGLOM_ID", Types.BIGINT,false),
                SystemColumnImpl.getColumn("PARTITION_ID",Types.VARCHAR,false),
                SystemColumnImpl.getColumn("COLUMN_ID",Types.INTEGER,false),
                SystemColumnImpl.getJavaColumn("DATA",
                        "com.splicemachine.stats.ColumnStatistics",false)
        };
    }
}
