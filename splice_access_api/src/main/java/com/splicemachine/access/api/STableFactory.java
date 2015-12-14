package com.splicemachine.access.api;

import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface STableFactory<Connection,Table,SpliceTableInfo> {
    public Table getTable(SpliceTableInfo tableName) throws IOException;
    public Table getTable(String name) throws IOException;

}
