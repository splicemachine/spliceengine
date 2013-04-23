package com.splicemachine.si.data.helper;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;

import java.io.IOException;

public class RelationHelper {
    private STable table;
    private final SDataLib dataLib;
    private final STableReader reader;
    private final STableWriter writer;

    public RelationHelper(SDataLib dataLib, STableReader reader, STableWriter writer) {
        this.dataLib = dataLib;
        this.reader = reader;
        this.writer = writer;
    }

    public void open(String tableName) throws IOException {
        table = reader.open(tableName);
    }

    public void write(Object[] keyParts, String family, Object qualifier, Object value, Long timestamp) throws IOException {
        final Object newKey = dataLib.newRowKey(keyParts);
        Object put = dataLib.newPut(newKey);
        dataLib.addKeyValueToPut(put, dataLib.encode(family), dataLib.encode(qualifier),
                timestamp, dataLib.encode(value));
        writer.write(table, put);
    }
}
