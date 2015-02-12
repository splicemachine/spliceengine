package com.splicemachine.derby.iapi.catalog;

/**
 * Created by jyuan on 2/6/15.
 */
import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

public class BackupRegionsDescriptor extends TupleDescriptor {

    private String backupItem;
    private String regionName;
    private long lastBackupTimestamp;

    public BackupRegionsDescriptor(String backupItem, String regionName, long lastBackupTimestamp) {
        this.backupItem = backupItem;
        this.regionName = regionName;
        this.lastBackupTimestamp = lastBackupTimestamp;
    }

    public String getBackupItem() {
        return backupItem;
    }

    public String getRegionName() {
        return regionName;
    }

    public long getLastBackupTimestamp() {
        return lastBackupTimestamp;
    }
}
