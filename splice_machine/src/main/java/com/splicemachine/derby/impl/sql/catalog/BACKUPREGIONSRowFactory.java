package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.BackupRegionsDescriptor;
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
public class BACKUPREGIONSRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "BACKUP_REGIONS";
    private static final int BACKUPREGIONS_COLUMN_COUNT = 3;

    private static final int BACKUP_ITEM = 1;
    private static final int REGION_NAME = 2;
    private static final int LAST_BACKUP_TIMESTAMP = 3;

    private static String uuids[] = {
            "9235cecb-d6b4-4ce7-bb08-745a8324f9d8",
            "9235cecb-d6b4-4ce7-bb08-745a8324f9d8"
    };

    public BACKUPREGIONSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPREGIONS_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String backup_item = null;
        String region_name = null;
        long lastBackupTimestamp = 0;

        if (td != null) {
            BackupRegionsDescriptor d = (BackupRegionsDescriptor)td;
            backup_item = d.getBackupItem();
            region_name = d.getRegionName();
            lastBackupTimestamp = d.getLastBackupTimestamp();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPREGIONS_COLUMN_COUNT);

        row.setColumn(BACKUP_ITEM, new SQLVarchar(backup_item));
        row.setColumn(REGION_NAME, new SQLVarchar(region_name));
        row.setColumn(LAST_BACKUP_TIMESTAMP, new SQLLongint(lastBackupTimestamp));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPREGIONS_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_REGIONS row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ITEM);
        String backupItem = col.getString();

        col = row.getColumn(REGION_NAME);
        String regionName = col.getString();

        col = row.getColumn(LAST_BACKUP_TIMESTAMP);
        long lastBackupTimestamp = col.getLong();

        return new BackupRegionsDescriptor(backupItem, regionName, lastBackupTimestamp);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ITEM", Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("REGION_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("LAST_BACKUP_TIMESTAMP",Types.BIGINT,false)
        };
    }
}
