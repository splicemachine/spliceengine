package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STableReader;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Iterator;

public class HTableReader implements STableReader<IHTable, Result, Get, Scan> {
    private final HTableSource tableSource;

    public HTableReader(HTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public IHTable open(String tableName) throws IOException {
        return new HbTable(tableSource.getTable(tableName));
    }

    @Override
    public void close(IHTable table) throws IOException {
        table.close();
    }

    @Override
    public Result get(IHTable table, Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public Iterator<Result> scan(IHTable table, Scan scan) throws IOException {
        return table.scan(scan);
    }
}
