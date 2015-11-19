package com.splicemachine.access.iapi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by jleach on 11/18/15.
 */
public interface SpliceTableFactory<Connection,Table,SpliceTableInfo> {
    public Table getTable(SpliceTableInfo tableName) throws IOException;
    public Table getTable(String name) throws IOException;

}
