package com.splicemachine.si.testsetup;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

import java.io.IOException;

public class RelationHelper {

    private Partition table;
    private final SDataLib dataLib;

    public RelationHelper(SDataLib dataLib) {
        this.dataLib = dataLib;
    }

    public void open(String tableName) throws IOException {
        table =SIDriver.getTableFactory().getTable(tableName);
    }

    public void write(Object[] keyParts, String family, Object qualifier, Object value, Long timestamp) throws IOException {
        final byte[] newKey = dataLib.newRowKey(keyParts);
        Object put = dataLib.newPut(newKey);
        dataLib.addKeyValueToPut(put, dataLib.encode(family), dataLib.encode(qualifier), timestamp, dataLib.encode(value));
        table.put(put);
    }
}
