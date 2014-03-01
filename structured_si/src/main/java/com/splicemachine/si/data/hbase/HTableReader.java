package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.utils.CloseableIterator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import java.io.IOException;

public class HTableReader implements STableReader<IHTable, Get, Scan> {
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
    public String getTableName(IHTable table) {
        return table.getName();
    }

    @Override
    public Result get(IHTable table, Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public CloseableIterator<Result> scan(IHTable table, Scan scan) throws IOException {
        return table.scan(scan);
    }

    @Override
    public void openOperation(IHTable table) throws IOException {
        table.startOperation();
    }

    @Override
    public void closeOperation(IHTable table) throws IOException {
       table.closeOperation();
    }

}
