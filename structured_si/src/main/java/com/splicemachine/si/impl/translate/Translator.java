package com.splicemachine.si.impl.translate;

import com.google.common.collect.Lists;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Convert data and operations from one "data store" to another.
 */
public class Translator<Data1, Result1, Put1 extends OperationWithAttributes, Delete1, Get1 extends OperationWithAttributes, Scan1, Table1,
				Data2, Result2, Put2 extends OperationWithAttributes, Delete2, Get2 extends OperationWithAttributes, Scan2, Table2, Mutation2> {

    private final SDataLib<Put1, Delete1, Get1, Scan1> dataLib1;
    private final STableReader<Table1, Get1, Scan1> reader1;

    private final SDataLib<Put2, Delete2, Get2, Scan2> dataLib2;
    private final STableReader<Table2, Get2, Scan2> reader2;
    private final STableWriter<Table2, Mutation2, Put2, Delete2> writer2;

    /**
     * Convert values from store 1 to store 2.
     */
    final Transcoder<Data1, Data2> transcoder;

    /**
     * Convert values from store 2 to store 1.
     */
    final Transcoder<Data2, Data1> transcoder2;

    public Translator(SDataLib<Put1, Delete1, Get1, Scan1> dataLib1,
                      STableReader<Table1, Get1, Scan1> reader1,
                      SDataLib<Put2, Delete2, Get2, Scan2> dataLib2,
                      STableReader<Table2, Get2, Scan2> reader2,
                      STableWriter<Table2, Mutation2, Put2, Delete2> writer2,
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
        final Iterator<Result> results = reader1.scan(table1, scan);
        while (results.hasNext()) {
            final List<KeyValue> keyValues = dataLib1.listResult(results.next());
            for (KeyValue kv : keyValues) {
								byte[] k2 = kv.getRow();
								byte[] f2 = kv.getFamily();
								byte[] q2 = kv.getQualifier();
								byte[] v2 = kv.getValue();
                final long timestamp = kv.getTimestamp();
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
        return dataLib2.newGet(dataLib1.getGetRow(get), null, null, null);
    }

    /**
     * Convert a Result object from store 2 representation into a store 1 representation.
     */
    public Result translateResult(Result result) {
        List<KeyValue> values = Lists.newArrayList();
				for(KeyValue kv : dataLib2.listResult(result)) {
            final KeyValue newKV = KeyValueUtils.newKeyValue(kv.getRow(),
										kv.getFamily(),
										kv.getQualifier(),
										kv.getTimestamp(),
										kv.getValue());
            values.add(newKV);
        }
				return new Result(values);
    }
}
