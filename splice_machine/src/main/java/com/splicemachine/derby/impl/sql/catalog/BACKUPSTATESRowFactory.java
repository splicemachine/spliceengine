package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.BackupRegionsDescriptor;
import com.splicemachine.derby.iapi.catalog.BackupStatesDescriptor;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.uuid.UUIDFactory;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * Created by jyuan on 2/6/15.
 */
public class BACKUPSTATESRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "BACKUP_STATES";
    private static final int BACKUPSTATES_COLUMN_COUNT = 4;

    private static final int BACKUP_ITEM = 1;
    private static final int REGION_NAME = 2;
    private static final int FILE_NAME = 3;
    private static final int STATE = 4;

    private static String uuids[] = {
            "cb4d2219-fd7a-4003-9e42-beff555a365c",
            "cb4d2219-fd7a-4003-9e42-beff555a365c"
    };

    public BACKUPSTATESRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPSTATES_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String backup_item = null;
        String region_name = null;
        String file_name = null;
        String state = null;

        if (td != null) {
            BackupStatesDescriptor d = (BackupStatesDescriptor)td;
            backup_item = d.getBackupItem();
            region_name = d.getRegionName();
            file_name = d.getFileName();
            state = d.getState();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPSTATES_COLUMN_COUNT);

        row.setColumn(BACKUP_ITEM, new SQLVarchar(backup_item));
        row.setColumn(REGION_NAME, new SQLVarchar(region_name));
        row.setColumn(FILE_NAME, new SQLVarchar(file_name));
        row.setColumn(STATE, new SQLVarchar(state));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPSTATES_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_STATES row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ITEM);
        String backupItem = col.getString();

        col = row.getColumn(REGION_NAME);
        String regionName = col.getString();

        col = row.getColumn(FILE_NAME);
        String fileName = col.getString();

        col = row.cloneColumn(STATE);
        String state = col.getString();

        return new BackupStatesDescriptor(backupItem, regionName, fileName, state);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ITEM", Types.VARCHAR, false, 32642),
                SystemColumnImpl.getColumn("REGION_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("FILE_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("STATE",Types.VARCHAR,false,32642)
        };
    }
}
