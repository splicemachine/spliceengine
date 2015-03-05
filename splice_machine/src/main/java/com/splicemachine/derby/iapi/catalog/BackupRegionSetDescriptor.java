package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * Created by jyuan on 2/11/15.
 */
public class BackupRegionSetDescriptor extends TupleDescriptor {

    private String backupItem;
    private String regionName;
    private String parentRegionName;

    public BackupRegionSetDescriptor (String backupItem, String regionName, String parentRegionName) {
        this.backupItem = backupItem;
        this.regionName = regionName;
        this.parentRegionName = parentRegionName;
    }

    public String getBackupItem() {
        return backupItem;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getParentRegionName() {
        return parentRegionName;
    }
}
