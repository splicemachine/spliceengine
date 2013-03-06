package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HStore implements HTableReaderI, HTableWriterI {
    private final HTableSource tableSource;

    public HStore(HTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public HTableInterface open(String relationIdentifier) {
        return tableSource.getTable(relationIdentifier);
    }

    private List toList(Object item) {
        return Arrays.asList(item);
    }

    @Override
    public Result get(HTableInterface table, Get get) {
        try {
            return table.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator scan(HTableInterface table, Scan scan) {
        try {
            final ResultScanner scanner = table.getScanner(scan);
            return scanner.iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close(HTableInterface table) {
        try {
            table.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(HTableInterface table, Put put) {
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(HTableInterface table, List puts) {
        try {
            table.put(puts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RowLock lockRow(HTableInterface table, byte[] row) {
        try {
            return table.lockRow(row);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unLockRow(HTableInterface table, RowLock lock) {
        try {
            table.unlockRow(lock);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean checkAndPut(HTableInterface table, byte[] family, byte[] qualifier, byte[] value, Put put) {
        try {
            return table.checkAndPut(put.getRow(), family, qualifier, value, put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
