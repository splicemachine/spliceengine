package com.splicemachine.derby.impl.sql.catalog;

/**
 * Created by jyuan on 5/9/14.
 */
public interface XPLAINTableDescriptor {
    String getTableName();
    String getTableDDL(String schemaName);
}