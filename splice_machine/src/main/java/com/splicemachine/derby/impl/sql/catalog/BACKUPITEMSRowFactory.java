package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.BackupItemsDescriptor;
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
import org.joda.time.DateTime;

import java.sql.Types;

/**
 * Created by jyuan on 2/6/15.
 */
public class BACKUPITEMSRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "BACKUP_ITEMS";
    private static final int BACKUPITEMS_COLUMN_COUNT = 4;

    private static final int TRANSACTION_ID = 1;
    private static final int ITEM = 2;
    private static final int BEGIN_TIMESTAMP = 3;
    private static final int END_TIMESTAMP = 4;

    private static String uuids[] = {
            "a0527143-4f6e-42df-98ab-b1dff6bea7db",
            "a0527143-4f6e-42df-98ab-b1dff6bea7db"
    };

    public BACKUPITEMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPITEMS_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long txnId = 0;
        String item = null;
        DateTime beginTimestamp = null;
        DateTime endTimestamp = null;

        if (td != null) {
            BackupItemsDescriptor d = (BackupItemsDescriptor)td;
            txnId = d.getTxnId();
            item = d.getItem();
            beginTimestamp = d.getBeginTimestamp();
            endTimestamp = d.getEndTimestamp();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPITEMS_COLUMN_COUNT);

        row.setColumn(TRANSACTION_ID, new SQLLongint(txnId));
        row.setColumn(ITEM, new SQLVarchar(item));
        row.setColumn(BEGIN_TIMESTAMP, new SQLTimestamp(beginTimestamp));
        row.setColumn(END_TIMESTAMP, new SQLTimestamp(endTimestamp));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPITEMS_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_ITEMS row");
        }

        DataValueDescriptor col = row.getColumn(TRANSACTION_ID);
        long txnId = col.getLong();

        col = row.getColumn(ITEM);
        String item = col.getString();

        col = row.getColumn(BEGIN_TIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        col = row.getColumn(END_TIMESTAMP);
        DateTime endTimestamp = col.getDateTime();

        return new BackupItemsDescriptor(txnId, item, beginTimestamp, endTimestamp);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("TRANSACTION_ID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("ITEM",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("BEGIN_TIMESTAMP",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("END_TIMESTAMP",Types.TIMESTAMP,true)
        };
    }
}
