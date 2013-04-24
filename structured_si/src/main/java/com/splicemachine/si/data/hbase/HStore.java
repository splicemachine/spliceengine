package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HStore implements IHTableReader, IHTableWriter {
    private final HTableSource tableSource;

    public HStore(HTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public HTableInterface open(String tableName) throws IOException {
        return tableSource.getTable(tableName);
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
    public Result get(HRegion region, Get get) {
        try {
            return region.get(get, null);
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
    public void write(HRegion region, Put put) {
        try {
            region.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(HRegion region, Put put, Integer lock) {
        try {
            region.put(put, lock);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Object table, Put put, boolean durable) {
        try {
            if (table instanceof HTableInterface) {
                ((HTableInterface) table).put(put);
            } else if (table instanceof HRegion) {
                ((HRegion) table).put(put, durable);
            }
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
    public RowLock lockRow(HTableInterface table, byte[] rowKey) {
        try {
            return table.lockRow(rowKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer lockRow(HRegion region, byte[] rowKey) {
        try {
            return region.obtainRowLock(rowKey);
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
    public void unLockRow(HRegion region, Integer lock) {
        region.releaseRowLock(lock);
    }

}
