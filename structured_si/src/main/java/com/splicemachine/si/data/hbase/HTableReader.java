package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

public class HTableReader implements STableReader<Result> {
    private final HTableSource tableSource;

    public HTableReader(HTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public STable open(String tableName) throws IOException {
        return new HbTable(tableSource.getTable(tableName));
    }

    @Override
    public void close(STable table) throws IOException {
        ((HbTable) table).table.close();
    }

    @Override
    public Result get(STable table, SGet get) throws IOException {
        if (table instanceof HbTable) {
            return ((HbTable) table).table.get(((HGet) get).get);
        } else {
            return ((HbRegion) table).region.get(((HGet) get).get, null);
        }
    }

    @Override
    public Iterator scan(STable table, SScan scan) throws IOException {
        final ResultScanner scanner = ((HbTable) table).table.getScanner(((HScan) scan).scan);
        return scanner.iterator();
    }
}
