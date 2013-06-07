package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STableReader;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;

public class HTableReader implements STableReader<IHTable, Result, HGet, HScan> {
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
    public Result get(IHTable table, HGet get) throws IOException {
        return table.get(get);
    }

    @Override
    public Iterator<Result> scan(IHTable table, HScan scan) throws IOException {
        return table.scan(scan);
    }
}
