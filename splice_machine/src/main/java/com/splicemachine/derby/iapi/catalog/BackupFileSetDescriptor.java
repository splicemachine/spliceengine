package com.splicemachine.derby.iapi.catalog;

import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * Created by jyuan on 2/6/15.
 */
public class BackupFileSetDescriptor extends TupleDescriptor {

    private String backupItem;
    private String regionName;
    private String fileName;
    private boolean include;

    public BackupFileSetDescriptor(String backupItem, String regionName, String fileName, boolean incldue) {
        this.backupItem = backupItem;
        this.regionName = regionName;
        this.fileName = fileName;
        this.include = include;
    }

    public String getBackupItem() {
        return backupItem;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getFileName() {
        return fileName;
    }

    public boolean shouldInclude() {
        return include;
    }
}
