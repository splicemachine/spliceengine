package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;
import com.splicemachine.si2.data.api.STable;
import com.splicemachine.si2.data.api.STableReader;

import java.util.Iterator;

public class HTableReaderAdapter implements STableReader {
    private final IHTableReader tableReader;

    public HTableReaderAdapter(IHTableReader tableReader) {
        this.tableReader = tableReader;
    }

    @Override
    public STable open(String tableName) {
        return new HbTable(tableReader.open(tableName));
    }

    @Override
    public void close(STable table) {
        tableReader.close(((HbTable) table).table);
    }

    @Override
    public Object get(STable table, SGet get) {
        if (table instanceof HbTable) {
            return tableReader.get(((HbTable) table).table, ((HGet) get).get);
        } else {
            return tableReader.get(((HbRegion) table).region, ((HGet) get).get);
        }
    }

    @Override
    public Iterator scan(STable table, SScan scan) {
        return tableReader.scan(((HbTable) table).table, ((HScan) scan).scan);
    }
}
