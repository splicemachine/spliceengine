package com.splicemachine.si2.data.helper;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;

import java.util.Arrays;

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

    public void open(String relationIdentifier) {
        table = reader.open(relationIdentifier);
    }

    public void write(Object[] keyParts, String family, Object qualifier, Object value, Long timestamp) {
        final Object newKey = dataLib.newRowKey(keyParts);
        Object tuple = dataLib.newPut(newKey);
        dataLib.addKeyValueToPut(tuple, dataLib.encode(family), dataLib.encode(qualifier),
                timestamp, dataLib.encode(value));
        writer.write(table, Arrays.asList(tuple));
    }
}
