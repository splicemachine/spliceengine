package com.splicemachine.access.iapi;

/**
 * Created by jleach on 11/18/15.
 */
public interface SpliceTableInfoFactory<TableInfo> {
    public TableInfo getTableInfo(String name);
    public TableInfo getTableInfo(byte[] name);

}