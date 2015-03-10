package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.ColumnStatsDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.derby.catalog.DefaultInfo;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.DefaultInfoImpl;
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
    public static final String TABLENAME_STRING = "SYSCOLUMNSTATS";
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

    public static ColumnDescriptor[] getViewColumns(TableDescriptor view,UUID viewId) throws StandardException{
        DataTypeDescriptor varcharType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR);
        DataTypeDescriptor longType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT);
        DataTypeDescriptor floatType = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.REAL);
        return new ColumnDescriptor[]{
                new ColumnDescriptor("SCHEMANAME"   ,1,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TABLENAME"    ,2,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("COLUMNNAME"   ,3,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("CARDINALITY"  ,4,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("NULL_COUNT"   ,5,longType,null,null,view,viewId,0,0),
                new ColumnDescriptor("NULL_FRACTION",6,floatType,null,null,view,viewId,0,0),
                new ColumnDescriptor("MIN_VALUE"    ,7,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("MAX_VALUE"    ,8,varcharType,null,null,view,viewId,0,0),
                new ColumnDescriptor("TOP_K"        ,9,varcharType,null,null,view,viewId,0,0),
        };
    }

    public static final String STATS_VIEW_SQL = "create view syscolumnstatistics as select " +
            "s.schemaname" +
            ",t.tablename" +
            ",co.columnname" +
            ",STATS_CARDINALITY(STATS_MERGE(data)) as CARDINALITY" +
            ",STATS_NULL_COUNT(STATS_MERGE(data)) as NULL_COUNT" +
            ",STATS_NULL_FRACTION(STATS_MERGE(data)) as NULL_FRACTION" +
            ",STATS_MIN(STATS_MERGE(data)) as MIN_VALUE" +
            ",STATS_MAX(STATS_MERGE(data)) as MAX_VALUE" +
            ",STATS_TOP_K(STATS_MERGE(data)) as TOP_K " +
            "from " +
            "sys.syscolumnstats cs" +
            ",sys.systables t" +
            ",sys.sysschemas s" +
            ",sys.sysconglomerates c" +
            ",sys.syscolumns co" +
            " where " +
            "t.tableid = c.tableid " +
            "and t.schemaid = s.schemaid " +
            "and c.conglomeratenumber = cs.conglom_id " +
            "and co.referenceid = t.tableid " +
            "and co.columnnumber = cs.column_id " +
            "and PARTITION_EXISTS(cs.conglom_id,partition_id) " +
            "group by " +
            "s.schemaname" +
            ",t.tablename" +
            ",co.columnname";
}
