package com.splicemachine.access.api;

import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * A Factory which can create Table instances
 * Created by jleach on 11/18/15.
 */
public interface STableFactory<SpliceTableInfo> {
    Partition getTable(SpliceTableInfo tableName) throws IOException;
    Partition getTable(String name) throws IOException;

}
