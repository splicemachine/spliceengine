package com.splicemachine.si.impl.translate;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Convert data and operations from one "data store" to another.
 */
public class Translator<Data1, Result1, KeyValue1, OperationWithAttributes1, Put1, Delete1, Get1, Scan1, Lock1, OperationStatus1, Table1, Scanner1,
        Data2, Result2, KeyValue2, OperationWithAttributes2, Put2, Delete2, Get2, Scan2, Lock2, OperationStatus2, Table2, Scanner2, Mutation2> {

    private final SDataLib<Data1, Result1, KeyValue1, OperationWithAttributes1, Put1, Delete1, Get1, Scan1, Lock1, OperationStatus1> dataLib1;
    private final STableReader<Table1, Result1, Get1, Scan1, KeyValue1, Scanner1, Data1> reader1;

    private final SDataLib<Data2, Result2, KeyValue2, OperationWithAttributes2, Put2, Delete2, Get2, Scan2, Lock2, OperationStatus2> dataLib2;
    private final STableReader<Table2, Result2, Get2, Scan2, KeyValue2, Scanner2, Data2> reader2;
    private final STableWriter<Table2, Mutation2, Put2, Delete2, Data2, Lock2, OperationStatus2> writer2;

    /**
     * Convert values from store 1 to store 2.
     */
    final Transcoder<Data1, Data2> transcoder;

    /**
     * Convert values from store 2 to store 1.
     */
    final Transcoder<Data2, Data1> transcoder2;

    public Translator(SDataLib<Data1, Result1, KeyValue1, OperationWithAttributes1, Put1, Delete1, Get1, Scan1, Lock1, OperationStatus1> dataLib1,
                      STableReader<Table1, Result1, Get1, Scan1, KeyValue1, Scanner1, Data1> reader1,
                      SDataLib<Data2, Result2, KeyValue2, OperationWithAttributes2, Put2, Delete2, Get2, Scan2, Lock2, OperationStatus2> dataLib2,
                      STableReader<Table2, Result2, Get2, Scan2, KeyValue2, Scanner2, Data2> reader2,
                      STableWriter<Table2, Mutation2, Put2, Delete2, Data2, Lock2, OperationStatus2> writer2,
                      Transcoder<Data1, Data2> transcoder,
                      Transcoder<Data2, Data1> transcoder2) {
        this.dataLib1 = dataLib1;
        this.reader1 = reader1;
        this.dataLib2 = dataLib2;
        this.reader2 = reader2;
        this.writer2 = writer2;
        this.transcoder = transcoder;
        this.transcoder2 = transcoder2;
    }

    /**
     * Read the contents of a table from store 1 and write it to store 2.
     */
    public void translate(String tableName) throws IOException {
        final Table1 table1 = reader1.open(tableName);
        final Table2 table2 = reader2.open(tableName);
        Scan1 scan = dataLib1.newScan(null, null, null, null, null);
        final Iterator<Result1> results = reader1.scan(table1, scan);
        while (results.hasNext()) {
            final List<KeyValue1> keyValues = dataLib1.listResult(results.next());
            for (KeyValue1 kv : keyValues) {
                final Data2 k2 = transcoder.transcodeKey(dataLib1.getKeyValueRow(kv));
                final Data2 f2 = transcoder.transcodeFamily(dataLib1.getKeyValueFamily(kv));
                final Data2 q2 = transcoder.transcodeQualifier(dataLib1.getKeyValueQualifier(kv));
                final long timestamp = dataLib1.getKeyValueTimestamp(kv);
                final Data2 v2 = transcoder.transcode(dataLib1.getKeyValueValue(kv));
                final Put2 put = dataLib2.newPut(k2);
                dataLib2.addKeyValueToPut(put, f2, q2, timestamp, v2);
                writer2.write(table2, put);
            }
        }
    }

    /**
     * Convert a Get on store 1 into an equivalent Get on store 2.
     */
    public Get2 translate(Get1 get) {
        return dataLib2.newGet(transcoder.transcodeKey(dataLib1.getGetRow(get)), null, null, null);
    }

    /**
     * Convert a Result object from store 2 representation into a store 1 representation.
     */
    public Result1 translateResult(Result2 result) {
        List<KeyValue1> values = new ArrayList<KeyValue1>();
        for(KeyValue2 kv : dataLib2.listResult(result)) {
            final KeyValue1 newKV = dataLib1.newKeyValue(transcoder2.transcodeKey(dataLib2.getKeyValueRow(kv)),
                    transcoder2.transcodeFamily(dataLib2.getKeyValueFamily(kv)),
                    transcoder2.transcodeQualifier(dataLib2.getKeyValueQualifier(kv)),
                    dataLib2.getKeyValueTimestamp(kv),
                    transcoder2.transcode(dataLib2.getKeyValueValue(kv)));
            values.add(newKV);
        }
        return dataLib1.newResult(transcoder2.transcodeKey(dataLib2.getResultKey(result)), values);
    }
}
