package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STableReader;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HTableReader implements STableReader<IHTable, Result, Get, Scan, KeyValue, RegionScanner, byte[]> {
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

    @Override
    public RegionScanner openRegionScanner(IHTable table, Scan scan) throws IOException {
        return table.startRegionScanner(scan);
    }

    @Override
    public List<KeyValue> nextResultsOnRegionScanner(RegionScanner regionScanner) throws IOException {
        List<KeyValue> result = new ArrayList<KeyValue>();
        regionScanner.nextRaw(result, null);
        return result;
    }

    @Override
    public void seekOnRegionScanner(RegionScanner regionScanner, byte[] rowKey) throws IOException {
        regionScanner.reseek(rowKey);
    }

    @Override
    public void closeRegionScanner(IHTable table) {
       table.closeScanner();
    }
}
