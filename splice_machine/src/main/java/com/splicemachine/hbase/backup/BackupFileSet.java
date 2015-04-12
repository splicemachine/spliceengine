package com.splicemachine.hbase.backup;

/**
 * Created by jyuan on 4/13/15.
 */
public class BackupFileSet {
    private String tableName;
    private String regionName;
    private String fileName;
    private boolean include;

    public BackupFileSet() {}

    public BackupFileSet(String tableName, String regionName, String fileName, boolean include) {
        this.tableName = tableName;
        this.regionName = regionName;
        this.fileName = fileName;
        this.include = include;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean shouldInclude() {
        return include;
    }

    public void setInclude(boolean include) {
        this.include = include;
    }
}
