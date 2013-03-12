package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.util.List;

public interface IHTableWriter {
    void write(HTableInterface table, Put put);
    void write(HRegion table, Put put);
    void write(HRegion region, Put put, Integer lock);
    void write(Object table, Put put, boolean durable);
    void write(HTableInterface table, List puts);

    boolean checkAndPut(HTableInterface table, byte[] family, byte[] qualifier, byte[] value, Put put);

    RowLock lockRow(HTableInterface table, byte[] rowKey);
    Integer lockRow(HRegion region, byte[] rowKey);
    void unLockRow(HTableInterface table, RowLock lock);
    void unLockRow(HRegion region, Integer lock);
}
