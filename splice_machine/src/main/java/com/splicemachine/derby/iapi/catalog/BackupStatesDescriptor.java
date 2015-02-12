package com.splicemachine.derby.iapi.catalog;

import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

/**
 * Created by jyuan on 2/6/15.
 */
public class BackupStatesDescriptor extends TupleDescriptor {

    private String backupItem;
    private String regionName;
    private String fileName;
    private String state;

    public BackupStatesDescriptor(String backupItem, String regionName, String fileName, String state) {
        this.backupItem = backupItem;
        this.regionName = regionName;
        this.fileName = fileName;
        this.state = state;
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

    public String getState() {
        return state;
    }
}
