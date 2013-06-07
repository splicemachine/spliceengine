package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SRowLock;
import com.splicemachine.si.data.api.STable;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

public class HTableWriter implements STableWriter<Put, Delete, byte[]> {

    @Override
    public void write(STable table, Put put) throws IOException {
        if (table instanceof HbTable) {
            ((HbTable) table).table.put(put);
        } else {
            ((HbRegion) table).region.put(put);
        }
    }

    @Override
    public void write(STable table, Put put, SRowLock rowLock) throws IOException {
        if (table instanceof HbTable) {
            ((HbTable) table).table.put(put);
        } else {
            ((HbRegion) table).region.put(put, ((HRowLock) rowLock).regionRowLock);
        }
    }

    @Override
    public void write(STable table, Put put, boolean durable) throws IOException {
        if (table instanceof HbTable) {
            if (((HbTable) table).table instanceof HTableInterface) {
                ((HbTable) table).table.put(put);
            } else if (((HbTable) table).table instanceof HRegion) {
                ((HRegion) ((HbTable) table).table).put(put, durable);
            } else {
                throw new RuntimeException("Cannot write to table of type " + ((HbTable) table).table.getClass().getName());
            }
        } else {
            if (((HbRegion) table).region instanceof HTableInterface) {
                ((HbRegion) table).region.put(put);
            } else if (((HbRegion) table).region instanceof HRegion) {
                ((HbRegion) table).region.put(put, durable);
            } else {
                throw new RuntimeException("Cannot write to table of type " + ((HbRegion) table).region.getClass().getName());
            }
        }
    }

    @Override
    public void write(STable table, List puts) throws IOException {
        ((HbTable) table).table.put(puts);
    }

    @Override
    public void delete(STable table, Delete delete, SRowLock rowLock) throws IOException {
        ((HbRegion) table).region.delete(delete, ((HRowLock) rowLock).regionRowLock, true);
    }

    @Override
    public boolean checkAndPut(STable table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return ((HbTable) table).table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public SRowLock lockRow(STable table, byte[] rowKey) throws IOException {
        if (table instanceof HbTable) {
            return new HRowLock(((HbTable) table).table.lockRow(rowKey));
        } else {
            final HRegion region = ((HbRegion) table).region;
            final Integer lock = region.obtainRowLock(rowKey);
            if (lock == null) {
                throw new RuntimeException("Unable to obtain row lock on region of table " + region.getTableDesc().getNameAsString());
            }
            return new HRowLock(lock);
        }
    }

    @Override
    public void unLockRow(STable table, SRowLock lock) throws IOException {
        if (table instanceof HbTable) {
            ((HbTable) table).table.unlockRow(((HRowLock) lock).lock);
        } else {
            ((HbRegion) table).region.releaseRowLock(((HRowLock) lock).regionRowLock);
        }
    }
}
