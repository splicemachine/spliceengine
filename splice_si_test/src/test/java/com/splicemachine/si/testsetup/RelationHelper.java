package com.splicemachine.si.testsetup;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.STableReader;
import com.splicemachine.si.api.data.STableWriter;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.io.IOException;

public class RelationHelper {

    private Object table;
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
        final byte[] newKey = dataLib.newRowKey(keyParts);
        Object put = dataLib.newPut(newKey);
        dataLib.addKeyValueToPut((OperationWithAttributes) put, dataLib.encode(family), dataLib.encode(qualifier),
                timestamp, dataLib.encode(value));
        writer.write(table, put);
    }
}
