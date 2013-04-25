package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableReader;

import java.io.IOException;
import java.util.Iterator;

public class HTableReaderAdapter implements STableReader {
    private final IHTableReader tableReader;

    public HTableReaderAdapter(IHTableReader tableReader) {
        this.tableReader = tableReader;
    }

    @Override
    public STable open(String tableName) throws IOException {
        return new HbTable(tableReader.open(tableName));
    }

    @Override
    public void close(STable table) throws IOException {
        tableReader.close(((HbTable) table).table);
    }

    @Override
    public Object get(STable table, SGet get) throws IOException {
        if (table instanceof HbTable) {
            return tableReader.get(((HbTable) table).table, ((HGet) get).get);
        } else {
            return tableReader.get(((HbRegion) table).region, ((HGet) get).get);
        }
    }

    @Override
    public Iterator scan(STable table, SScan scan) throws IOException {
        return tableReader.scan(((HbTable) table).table, ((HScan) scan).scan);
    }
}
