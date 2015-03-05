package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.iapi.catalog.BackupRegionSetDescriptor;
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
import com.splicemachine.db.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * Created by jyuan on 2/11/15.
 */
public class BACKUPREGIONSETRowFactory extends CatalogRowFactory {

    private static final String TABLENAME_STRING = "BACKUP_REGIONSET";
    private static final int BACKUPREGIONSET_COLUMN_COUNT = 3;
    private static final int BACKUP_ITEM = 1;
    private static final int REGION_NAME = 2;
    private static final int PARENT_REGION_NAME = 3;

    private String[] uuids = {
            "6626c797-9586-44de-ad97-03f6c5acbb3a",
            "6626c797-9586-44de-ad97-03f6c5acbb3a"
    };

    public BACKUPREGIONSETRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf) {
        super(uuidf, ef, dvf);
        initInfo(BACKUPREGIONSET_COLUMN_COUNT, TABLENAME_STRING, null, null, uuids);
    }

    @Override
    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent) throws StandardException {

        String backup_item = null;
        String region_name = null;
        String parent_region_name = null;

        if (td != null) {
            BackupRegionSetDescriptor d = (BackupRegionSetDescriptor) td;
            backup_item = d.getBackupItem();
            region_name = d.getRegionName();
            parent_region_name = d.getParentRegionName();
        }

        ExecRow row = getExecutionFactory().getValueRow(BACKUPREGIONSET_COLUMN_COUNT);

        row.setColumn(BACKUP_ITEM, new SQLVarchar(backup_item));
        row.setColumn(REGION_NAME, new SQLVarchar(region_name));
        row.setColumn(PARENT_REGION_NAME, new SQLVarchar(parent_region_name));

        return row;
    }

    @Override
    public TupleDescriptor buildDescriptor(ExecRow row,
                                           TupleDescriptor parentTuple,
                                           DataDictionary dataDictionary) throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                    row.nColumns() == BACKUPREGIONSET_COLUMN_COUNT,
                    "Wrong number of columns for a BACKUP_ITEMS row");
        }

        DataValueDescriptor col = row.getColumn(BACKUP_ITEM);
        String backup_item= col.getString();

        col = row.getColumn(REGION_NAME);
        String region_name= col.getString();

        col = row.getColumn(PARENT_REGION_NAME);
        String parent_region_name= col.getString();

        return new BackupRegionSetDescriptor(backup_item, region_name, parent_region_name);
    }

    @Override
    public SystemColumn[] buildColumnList() throws StandardException {
        return new SystemColumn[]{
                SystemColumnImpl.getColumn("BACKUP_ITEM",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("REGION_NAME",Types.VARCHAR,false,32642),
                SystemColumnImpl.getColumn("PARENT_REGION_NAME",Types.VARCHAR,true,32642),
        };
    }
}
