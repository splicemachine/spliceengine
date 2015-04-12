package com.splicemachine.derby.impl.sql.catalog;

/**
 * Created by jyuan on 2/6/15.
 */
import com.splicemachine.derby.iapi.catalog.BackupDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;
import org.joda.time.DateTime;

import java.sql.Types;

public class BACKUPRowFactory extends CatalogRowFactory {
    private static final String TABLENAME_STRING = "SYSBACKUP";
    private static final int BACKUP_COLUMN_COUNT = 9;

    private static final int BACKUP_ID = 1;
    private static final int BEGIN_TIMESTAMP = 2;
    private static final int END_TIMESTAMP = 3;
    private static final int STATUS = 4;
    private static final int FILESYSTEM = 5;
    private static final int SCOPE = 6;
    private static final int IS_INCREMENTAL_BACKUP = 7;
    private static final int INCREMENTAL_PARENT_BACKUP_ID = 8;
    private static final int ITEMS = 9;

    private static String uuids[] = {
            "6e205c88-4c1b-464a-b0ab-865d64b3279d",
            "6e205c88-4c1b-464a-b0ab-865d64b3279d"
    };

    public BACKUPRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUP_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        long backupId = 0;
        DateTime beginTimestamp = null;
        DateTime endTimestamp = null;
        String status = null;
        String fileSystem = null;
        String scope = null;
        boolean isIncremental = false;
        long parentId = -1;
        int items = 0;

        if (td != null) {
            BackupDescriptor d = (BackupDescriptor)td;
            backupId = d.getBackupId();
            beginTimestamp = d.getBeginTimestamp();
            endTimestamp = d.getEndTimestamp();
            status = d.getStatus();
            fileSystem = d.getFileSystem();
            scope = d.getScope();
            isIncremental = d.isIncremental();
            parentId = d.getParentBackupId();
            items = d.getItems();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUP_COLUMN_COUNT);

        row.setColumn(BACKUP_ID, new SQLLongint(backupId));
        row.setColumn(BEGIN_TIMESTAMP, new SQLTimestamp(beginTimestamp));
        row.setColumn(END_TIMESTAMP, new SQLTimestamp(endTimestamp));
        row.setColumn(STATUS, new SQLVarchar(status));
        row.setColumn(FILESYSTEM, new SQLVarchar(fileSystem));
        row.setColumn(SCOPE, new SQLVarchar(scope));
        row.setColumn(IS_INCREMENTAL_BACKUP, new SQLBoolean(isIncremental));
        row.setColumn(INCREMENTAL_PARENT_BACKUP_ID, new SQLDouble(parentId));
        row.setColumn(ITEMS, new SQLInteger(items));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUP_COLUMN_COUNT,
                    "Wrong number of columns for a SYSBACKUP row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ID);
        long backupId = col.getLong();

        col = row.getColumn(BEGIN_TIMESTAMP);
        DateTime beginTimestamp = col.getDateTime();

        col = row.getColumn(END_TIMESTAMP);
        DateTime endTimestamp = col.getDateTime();

        col = row.getColumn(STATUS);
        String status = col.getString();

        col = row.getColumn(FILESYSTEM);
        String fileSystem = col.getString();

        col = row.getColumn(SCOPE);
        String scope = col.getString();

        col = row.getColumn(IS_INCREMENTAL_BACKUP);
        boolean isIncremental = col.getBoolean();

        col = row.getColumn(INCREMENTAL_PARENT_BACKUP_ID);
        long parentId = col.getLong();

        col = row.getColumn(ITEMS);
        int items = col.getInt();

        return new BackupDescriptor(backupId, beginTimestamp, endTimestamp, status, fileSystem,
                scope, isIncremental, parentId, items);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ID", Types.BIGINT, false),
                SystemColumnImpl.getColumn("BEGIN_TIMESTAMP",Types.TIMESTAMP,false),
                SystemColumnImpl.getColumn("END_TIMESTAMP",Types.TIMESTAMP,true),
                SystemColumnImpl.getColumn("STATUS",Types.VARCHAR,false,10),
                SystemColumnImpl.getColumn("FILESYSTEM",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("SCOPE",Types.VARCHAR,false,10),
                SystemColumnImpl.getColumn("INCREMENTAL_BACKUP",Types.BOOLEAN,false),
                SystemColumnImpl.getColumn("INCREMENTAL_PARENT_BACKUP_ID",Types.BIGINT,true),
                SystemColumnImpl.getColumn("BACKUP_ITEM",Types.INTEGER, true),
        };
    }
}