package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.derby.iapi.catalog.BackupFileSetDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * Created by jyuan on 2/6/15.
 */
public class BACKUPFILESETRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "SYSBACKUPFILESET";
    private static final int BACKUPFILESET_COLUMN_COUNT = 4;

    private static final int BACKUP_ITEM = 1;
    private static final int REGION_NAME = 2;
    private static final int FILE_NAME = 3;
    private static final int INCLUDE = 4;

    private static String uuids[] = {
            "cb4d2219-fd7a-4003-9e42-beff555a365c",
            "cb4d2219-fd7a-4003-9e42-beff555a365c"
    };

    public BACKUPFILESETRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPFILESET_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String backup_item = null;
        String region_name = null;
        String file_name = null;
        boolean include = false;

        if (td != null) {
            BackupFileSetDescriptor d = (BackupFileSetDescriptor)td;
            backup_item = d.getBackupItem();
            region_name = d.getRegionName();
            file_name = d.getFileName();
            include = d.shouldInclude();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPFILESET_COLUMN_COUNT);

        row.setColumn(BACKUP_ITEM, new SQLVarchar(backup_item));
        row.setColumn(REGION_NAME, new SQLVarchar(region_name));
        row.setColumn(FILE_NAME, new SQLVarchar(file_name));
        row.setColumn(INCLUDE, new SQLBoolean(include));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPFILESET_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_FILESET row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ITEM);
        String backupItem = col.getString();

        col = row.getColumn(REGION_NAME);
        String regionName = col.getString();

        col = row.getColumn(FILE_NAME);
        String fileName = col.getString();

        col = row.cloneColumn(INCLUDE);
        boolean include = col.getBoolean();

        return new BackupFileSetDescriptor(backupItem, regionName, fileName, include);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ITEM", Types.VARCHAR, false, 32642),
                SystemColumnImpl.getColumn("REGION_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("FILE_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("INCLUDE",Types.BOOLEAN,false)
        };
    }
}
