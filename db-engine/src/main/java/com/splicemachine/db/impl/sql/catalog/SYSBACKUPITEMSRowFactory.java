package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import org.joda.time.DateTime;
import java.sql.Types;

/**
 * Created by jyuan on 2/6/15.
 */
public class SYSBACKUPITEMSRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSBACKUPITEMS";
    private static final int BACKUPITEMS_COLUMN_COUNT = 4;

    private static final int BACKUP_ID = 1;
    private static final int ITEM = 2;
    private static final int BEGIN_TIMESTAMP = 3;
    private static final int END_TIMESTAMP = 4;

    protected static final int SYSBACKUPITEMS_INDEX1_ID = 0;

    private	static	final	boolean[]	uniqueness = {
            true
    };

    private static final int[][] indexColumnPositions = {
                    {BACKUP_ID, ITEM}
            };


    private static String uuids[] = {
            "a0527143-4f6e-42df-98ab-b1dff6bea7db",
            "a0527143-4f6c-42df-98ab-b1dff6bea7db",
            "a0527143-4f6z-42df-98ab-b1dff6bea7db"
    };

    public SYSBACKUPITEMSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPITEMS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long backupId = 0;
        String item = null;
        DateTime beginTimestamp = null;
        DateTime endTimestamp = null;

        if (td != null) {
            BackupItemsDescriptor d = (BackupItemsDescriptor)td;
            backupId = d.getBackupId();
            item = d.getItem();
            beginTimestamp = d.getBeginTimestamp();
            endTimestamp = d.getEndTimestamp();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPITEMS_COLUMN_COUNT);

        row.setColumn(BACKUP_ID, new SQLLongint(backupId));
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
                    "Wrong number of columns for a SYSBACKUPITEMS row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ID);
        long backupId = col.getLong();

        col = row.getColumn(ITEM);
        String item = col.getString();

        col = row.getColumn(BEGIN_TIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        col = row.getColumn(END_TIMESTAMP);
        DateTime endTimestamp = col.getDateTime();

        return new BackupItemsDescriptor(backupId, item, beginTimestamp, endTimestamp);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("ITEM",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("BEGIN_TIMESTAMP",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("END_TIMESTAMP",Types.TIMESTAMP,true),
        };
    }
}
