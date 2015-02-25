package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.ColumnStatsDescriptor;
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
    private static final int SYSCOLUMNSTATISTICS_COLUMN_COUNT = 11;
    private static final int CONGLOMID      = 1;
    private static final int PARTITIONID    = 2;
    private static final int COLUMNID       = 3;
    private static final int NULLCOUNT      = 4;
    private static final int CARDINALITY    = 5;
    private static final int FREQUENTELEMS  = 6;
    private static final int DISTRIBUTION   = 7;
    private static final int MINVAL         = 8;
    private static final int MINFREQ        = 9;
    private static final int MAXVAL         = 10;
    private static final int MAXFREQ        = 11;

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
        row.setColumn(NULLCOUNT,new SQLLongint());
        row.setColumn(CARDINALITY,new SQLBit());
        row.setColumn(FREQUENTELEMS,new SQLBit());
        row.setColumn(DISTRIBUTION,new SQLBit());
        row.setColumn(MINVAL,new SQLBit());
        row.setColumn(MINFREQ,new SQLLongint());
        row.setColumn(MAXVAL,new SQLBit());
        row.setColumn(MAXFREQ,new SQLLongint());

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row, TupleDescriptor parentTuple, DataDictionary dataDictionary) throws StandardException {
        DataValueDescriptor col = row.getColumn(CONGLOMID);
        long conglomId = col.getLong();
        col = row.getColumn(PARTITIONID);
        String partitionId = col.getString();
        col = row.getColumn(COLUMNID);
        long colNum = col.getInt();
        col = row.getColumn(NULLCOUNT);
        long nullCount = col.getInt();
        col = row.getColumn(CARDINALITY);
        byte[] card = col.getBytes();
        col = row.getColumn(FREQUENTELEMS);
        byte[] freqs = col.getBytes();
        col = row.getColumn(DISTRIBUTION);
        byte[] dist = col.getBytes();
        col = row.getColumn(MINVAL);
        byte[] min = col.getBytes();
        col = row.getColumn(MINFREQ);
        long minFreq = col.getLong();
        col = row.getColumn(MAXVAL);
        byte[] max = col.getBytes();
        col = row.getColumn(MAXFREQ);
        long maxFreq = col.getLong();

        return new ColumnStatsDescriptor(conglomId,
                partitionId,
                colNum,
                nullCount,
                card,
                freqs,
                dist,
                min,
                minFreq,
                max,
                maxFreq);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("CONGLOM_ID", Types.BIGINT,false),
                SystemColumnImpl.getColumn("PARTITION_ID",Types.VARCHAR,false),
                SystemColumnImpl.getColumn("COLUMN_ID",Types.INTEGER,false),
                SystemColumnImpl.getColumn("NULLCOUNT",Types.BIGINT,true),
                SystemColumnImpl.getColumn("CARDINALITY",Types.BINARY,true),
                SystemColumnImpl.getColumn("FREQUENTELEMS",Types.VARBINARY,true),
                SystemColumnImpl.getColumn("DISTRIBUTION",Types.VARBINARY,true),
                SystemColumnImpl.getColumn("MINVAL",Types.VARBINARY,true),
                SystemColumnImpl.getColumn("MINFREQ",Types.BIGINT,true),
                SystemColumnImpl.getColumn("MAXVAL",Types.VARBINARY,true),
                SystemColumnImpl.getColumn("MAXFREQ",Types.BIGINT,true)
        };
    }
}
