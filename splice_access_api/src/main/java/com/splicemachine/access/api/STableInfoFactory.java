package com.splicemachine.access.api;

/**
 * Created by jleach on 11/18/15.
 */
public interface STableInfoFactory<TableInfo> {
    public TableInfo getTableInfo(String name);
    public TableInfo getTableInfo(byte[] name);
    public TableInfo parseTableInfo(String namespacePlusTable);
}